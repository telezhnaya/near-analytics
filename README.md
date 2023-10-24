# near-analytics

Analytics Tool for NEAR Blockchain.  
[NEAR Explorer](https://explorer.near.org/) uses it for [mainnet](https://explorer.near.org/stats) and [testnet](https://explorer.testnet.near.org/stats).

Keep in mind that the data (both format and the contents) could be changed at any time, the tool is under development.

<img width="918" alt="Example of data" src="https://user-images.githubusercontent.com/11246099/135101272-61fe872f-2129-455d-aee1-00d0f4570900.png">

### Install

```bash
sudo apt install python3.9-distutils libpq-dev python3.9-dev postgresql-server-dev-all

python3.9 -m pip install --upgrade pip
python3.9 -m pip install -r requirements.txt
```

### Run

```bash
python3.9 main.py -h
```

### Contribute

See [Contributing Guide](CONTRIBUTING.md) for details

### Usage examples

Apart from [NEAR Explorer](https://explorer.near.org/stats), see [nice blogpost](https://analyticali.substack.com/p/near-analytics-cheatsheet) with the tutorial and beautiful pictures based on NEAR Analytics data.
