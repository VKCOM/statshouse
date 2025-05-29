# StatsHouse UI

## Requirements

* Node 20+

## Working with remote instance of StatsHouse

It is useful to use remote backend, so you can work with actual data.
To do that you need to set up proxy to that instance.

### Setting up proxy

```shell
cp .env .env.local
echo REACT_APP_PROXY=https://statshouse.example.org/ >> .env.local
echo REACT_APP_PROXY_COOKIE="copy Cookie http response header from the instance" >> .env.local
echo REACT_APP_CONFIG="copy <meta name='setting' content='...'/> content from StatsHouse page" >> .env.local
```

## Run dev mode

```shell
npm install
npm run start
```
