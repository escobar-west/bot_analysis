This Rust program subscribes to transactions involving a given account and writes data to a database for downstream data analysis.

```
$ cargo run -- -e "https://temporal.rpcpool.com" --x-token "4586b536-c930-43f1-91c1-bee31ab3a0a2" --accounts "MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz"
2025-08-01T14:14:14Z WARN  sqlx_postgres::options::parse] ignoring unrecognized connect parameter key=channel_binding value=require
[2025-08-01T14:14:14Z INFO  bot_analysis] Connected
[2025-08-01T14:14:14Z INFO  bot_analysis] stream opened
[2025-08-01T14:14:15Z INFO  bot_analysis] transaction (client) at 1754057643.203866: {"fee":38363,"signer":"77777T2qnynHFsA63FyfY766ciBTXizavU1f5HeZXwN","txn_hash":"4fpK5FfdvzNL2X6HaqKGgrAX6WCkk3squDn4suKN3vdgKh69UAdcPcBhMRZqMLpfoKBY8bn6UMiAW3z7wQgbo2GH","unix_epoch":1754057643}
[2025-08-01T14:14:15Z INFO  bot_analysis] transaction (client) at 1754057643.351329: {"fee":5000,"signer":"77777T2qnynHFsA63FyfY766ciBTXizavU1f5HeZXwN","txn_hash":"4LagaT8V7W6Dz2mxH1pp9pMNLLcd9emAfwkUNgcjhkkp4h7nszPXsau3dSjfuvf2QhQoDEWqmkVPLtFq3CtuiThC","unix_epoch":1754057643}
```

![Alt text](img/screenshot.png?raw=true "Code output")