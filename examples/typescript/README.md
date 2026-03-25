# Usage

Install `tsx` globally `npm i -g tsx`

Specify an dotenv file (`.env`) with two to environment variable:

```
FUMAROLE_ENDPOINT="https://ams.rpcpool.com"
FUMAROLE_X_TOKEN="<YOUR X_TOKEN>"
FUMAROLE_CLIENT_LOG_LEVEL="info"
```

```sh
cd typescript-sdk && pnpm install && pnpm run build
cd ../examples/typescript
pnpm install

tsx src/subscribe-token-transactions.ts 
```
