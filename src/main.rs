use {
    anyhow::Context,
    backoff::{ExponentialBackoff, future::retry},
    clap::{Parser, ValueEnum},
    futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt},
    log::{error, info},
    serde_json::{Value, json},
    solana_signature::Signature,
    sqlx::postgres::PgPool,
    std::{
        collections::HashMap,
        env,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::sync::Mutex,
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        convert_from::create_pubkey_vec,
        prelude::{
            CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions,
            SubscribeRequestPing, SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
        },
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    /// Filter included account in transactions
    #[clap(long)]
    accounts: Vec<String>,

    /// Commitment level: processed, confirmed or finalized
    #[clap(long)]
    commitment: Option<ArgsCommitment>,
}

impl Args {
    fn get_commitment(&self) -> Option<CommitmentLevel> {
        Some(ArgsCommitment::default().into())
    }

    async fn connect(&self) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        let tls_config = ClientTlsConfig::new().with_enabled_roots();
        let builder = GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(self.x_token.clone())?
            .tls_config(tls_config)?;
        builder.connect().await.map_err(Into::into)
    }
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum ArgsCommitment {
    Processed,
    Confirmed,
    #[default]
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

fn get_subscribe_request(
    args: &Args,
    commitment: Option<CommitmentLevel>,
) -> anyhow::Result<Option<SubscribeRequest>> {
    Ok({
        let mut transactions = HashMap::new();
        transactions.insert(
            "client".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: args.accounts.clone(),
                account_exclude: vec![],
                account_required: vec![],
            },
        );
        Some(SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: commitment.map(|x| x as i32),
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        })
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe {
        env::set_var(
            env_logger::DEFAULT_FILTER_ENV,
            env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
        );
    }
    env_logger::init();

    let args = Args::parse();
    let zero_attempts = Arc::new(Mutex::new(true));

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let args = args.clone();
        let zero_attempts = Arc::clone(&zero_attempts);

        async move {
            let pool = PgPool::connect(&env::var("POSTGRES_DB_URL").unwrap())
                .await
                .unwrap();

            let mut zero_attempts = zero_attempts.lock().await;
            if *zero_attempts {
                *zero_attempts = false;
            } else {
                info!("Retry to connect to the server");
            }
            drop(zero_attempts);

            let commitment = args.get_commitment();
            let client = args.connect().await.map_err(backoff::Error::transient)?;
            info!("Connected");
            let request = get_subscribe_request(&args, commitment)
                .map_err(backoff::Error::Permanent)?
                .ok_or(backoff::Error::Permanent(anyhow::anyhow!(
                    "expect subscribe action"
                )))?;

            geyser_subscribe(pool, client, request)
                .await
                .map_err(backoff::Error::transient)?;

            Ok::<(), backoff::Error<anyhow::Error>>(())
        }
        .inspect_err(|error| error!("failed to connect: {error}"))
    })
    .await
    .map_err(Into::into)
}

async fn geyser_subscribe(
    pool: PgPool,
    mut client: GeyserGrpcClient<impl Interceptor>,
    request: SubscribeRequest,
) -> anyhow::Result<()> {
    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;
    info!("stream opened");
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                let filters = msg.filters;
                let created_at: SystemTime = msg
                    .created_at
                    .ok_or(anyhow::anyhow!("no created_at in the message"))?
                    .try_into()
                    .context("failed to parse created_at")?;
                match msg.update_oneof {
                    Some(UpdateOneof::Transaction(msg)) => {
                        let tx = msg
                            .transaction
                            .ok_or(anyhow::anyhow!("no transaction in the message"))?;
                        let mut value = create_pretty_transaction(tx)?;
                        value["unix_epoch"] = created_at
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                            .into();
                        add_record(&pool, &value).await.unwrap();
                        print_update("transaction", created_at, &filters, value);
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        // This is necessary to keep load balancers that expect client pings alive. If your load balancer doesn't
                        // require periodic client pings then this is unnecessary
                        subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await?;
                    }
                    Some(UpdateOneof::Pong(_)) => {}
                    None => {
                        error!("update not found in the message");
                        break;
                    }
                    _ => {
                        let msg = msg.update_oneof;
                        error!("unexpected update message: {msg:#?}");
                        break;
                    }
                }
            }
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }
    }
    info!("stream closed");
    Ok(())
}

fn create_pretty_transaction(tx: SubscribeUpdateTransactionInfo) -> anyhow::Result<Value> {
    let signer: String = create_pubkey_vec(
        tx.transaction
            .as_ref()
            .map(|t| t.message.as_ref().map(|m| m.account_keys.clone()))
            .flatten()
            .unwrap(),
    )
    .unwrap()[0]
        .to_string();
    let fee = tx.clone().meta.unwrap().fee;
    Ok(json!({
        "txn_hash": Signature::try_from(tx.signature.as_slice()).context("invalid signature")?.to_string(),
        "signer": signer,
        "fee": fee,
    }))
}

fn print_update(kind: &str, created_at: SystemTime, filters: &[String], value: Value) {
    let unix_since = created_at
        .duration_since(UNIX_EPOCH)
        .expect("valid system time");
    info!(
        "{kind} ({}) at {}.{:0>6}: {}",
        filters.join(","),
        unix_since.as_secs(),
        unix_since.subsec_micros(),
        serde_json::to_string(&value).expect("json serialization failed")
    );
}

async fn add_record(pool: &PgPool, data: &Value) -> anyhow::Result<()> {
    let sql_query = format!(
        r#"INSERT INTO txns ( txn_hash, unix_epoch, signer, fee ) VALUES ( '{}', {}, '{}', {} )"#,
        data["txn_hash"].as_str().unwrap(),
        data["unix_epoch"].as_i64().unwrap(),
        data["signer"].as_str().unwrap(),
        data["fee"].as_i64().unwrap()
    );
    sqlx::query(&sql_query).execute(pool).await?;
    Ok(())
}
