use crate::ContractType::ERC20;

use dotenv::dotenv;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use web3::ethabi::{Event, EventParam, ParamType, RawLog};
use web3::transports::WebSocket;
use web3::types::{BlockId, BlockNumber, Log};
use web3::Web3;
use influxdb::{Client, Query, Timestamp, ReadQuery};
use influxdb::InfluxDbWriteable;
use chrono::{DateTime, Utc, NaiveDateTime};

#[derive(InfluxDbWriteable)]
pub struct TransferOnly {
    pub time: DateTime<Utc>,
    pub block: String,
    pub from: String,
    pub to: String,
    pub txhash: String,
    pub value: String,
}

const ERC_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Serialize, Deserialize)]
pub struct Contract {
    pub name: &'static str,
    pub decimals: usize,
    pub erc: ContractType,
    pub address: &'static str,
}

pub fn to_string<T: serde::Serialize>(request: &T) -> String {
    web3::helpers::to_string(request).replace('\"', "")
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Debug, Clone)]
pub enum ContractType {
    ERC20,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Transfer {
    contract: String,
    from: String,
    to: String,
    value: String,
    timestamp: u64,
}

async fn scrape_block(
    provider: &WebSocket,
    current_block: u64,
    contracts_of_interest: &[&str; 3],
    map: &HashMap<&str, Contract>,
    event: &Event,
    client: &Client,
) {
    let web3 = Web3::new(provider);

    let chain_head_block = web3
        .eth()
        .block_number()
        .await
        .expect("Failed to retrieve head block number from chain!");

    let block = web3
        .eth()
        .block_with_txs(BlockId::Number(BlockNumber::from(current_block as u64)))
        .await
        .unwrap_or_else(|_| panic!("Failed to load block {} from provider!", current_block))
        .unwrap_or_else(|| panic!("Failed to unwrap block {} from result!", current_block));

    let contracts: Vec<&str> = map
        .values()
        .filter(|c| c.erc == ERC20)
        .map(|c| c.address)
        .collect();

    let mut count = 0u32;

    for tx in block.transactions {
        if let Some(tx_to) = tx.to {
            let tx_to = to_string(&tx_to);

            if contracts_of_interest.contains(&tx_to.as_str()) {
                let action = web3.eth().transaction_receipt(tx.hash).await.unwrap();

                if (action.is_none() == false) {
                    let receipt = action.unwrap();

                    let transfer_log = receipt
                        .logs
                        .iter()
                        .filter(|x| {
                            to_string(&x.topics[0]) == ERC_TRANSFER_TOPIC
                                && contracts.contains(&to_string(&x.address).as_str())
                        })
                        .collect::<Vec<&Log>>();

                    for transfer in transfer_log {
                        let data = event
                            .parse_log(RawLog {
                                topics: transfer.to_owned().topics,
                                data: transfer.to_owned().data.0,
                            })
                            .unwrap();

                        let from = to_string(&data.params[0].value.to_string());
                        let to = to_string(&data.params[1].value.to_string());
                        let value = to_string(&data.params[2].value.to_string());

                        let timestamp = block.timestamp.to_string().parse::<i64>().unwrap();
                        let naive = NaiveDateTime::from_timestamp(timestamp, 0);
                        let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);

                        let transfer = TransferOnly {
                            time: datetime,
                            block: current_block.to_string(),
                            from,
                            to,
                            txhash: tx.hash.to_string(),
                            value,
                        };

                        println!("Got data");

                        let write_result = client
                                                                            .query(transfer.into_query(&tx_to.clone()))
                                                                            .await;
                        
                        assert!(write_result.is_ok(), "Write result was not okay");



                    }
                } else {
                    println!("Null");
                }
            }
        };
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let PROVIDER_URL = std::env::var("PROVIDER_URL").expect("PROVIDER_URL must be set.");
    let INFLUXDB_TOKEN = std::env::var("INFLUXDB_TOKEN").expect("INFLUXDB_TOKEN must be set.");

    let client = Client::new("https://us-east-1-1.aws.cloud2.influxdata.com", "metaportalweb@gmail.com").with_auth("metaportalweb@gmail.com", INFLUXDB_TOKEN);
    let det = client.ping().await.unwrap();

    println!("Autenticated Succesfully to influx server: {} {}", det.0, det.1);


    let provider = web3::transports::WebSocket::new(&PROVIDER_URL)
        .await
        .unwrap();

    let mut map = HashMap::new();

    let contracts_of_interest = [
        "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
        "97a9107c1793bc407d6f527b77e7fff4d812bece",
        "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
    ];

    map.insert(
        "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
        Contract {
            name: "WETH",
            decimals: 18,
            erc: ContractType::ERC20,
            address: "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
        },
    );

    map.insert(
        "0x97a9107c1793bc407d6f527b77e7fff4d812bece",
        Contract {
            name: "AXS",
            decimals: 18,
            erc: ContractType::ERC20,
            address: "97a9107c1793bc407d6f527b77e7fff4d812bece",
        },
    );

    map.insert(
        "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
        Contract {
            name: "SLP",
            decimals: 0,
            erc: ContractType::ERC20,
            address: "0xa8754b9fa15fc18bb59458815510e40a12cd2014",
        },
    );

    let event = Event {
        name: "Transfer".to_string(),
        inputs: vec![
            EventParam {
                name: "_from".to_string(),
                kind: ParamType::Address,
                indexed: true,
            },
            EventParam {
                name: "_to".to_string(),
                kind: ParamType::Address,
                indexed: true,
            },
            EventParam {
                name: "_value".to_string(),
                kind: ParamType::Uint(256),
                indexed: false,
            },
        ],
        anonymous: false,
    };

    let at_once = 150;

    let mut current_block = 15000000u64;


    for element in contracts_of_interest {
        client
            .query(ReadQuery::new( format!("CREATE DATABASE {}", element)))
            .await
            .expect("could not setup db");

        println!("Created Database: {}", element);
    }


    loop {
        let mut calls = Vec::new();

        let chain_head_block = Web3::new(&provider)
            .eth()
            .block_number()
            .await
            .expect("Failed to retrieve head block number from chain!")
            .as_u64()
            - (at_once + 50);

        if chain_head_block < current_block {
            break;
        }

        let starting_block = current_block;

        loop {
            let mut call = scrape_block(
                &provider,
                current_block,
                &contracts_of_interest,
                &map,
                &event,
                &client,
            );
            calls.push(call);

            current_block = current_block + 1;

            if (current_block > starting_block + at_once) {
                break;
            }
        }

        join_all(calls).await;
        println!("Completed a thread: {}", current_block);
    }

}
