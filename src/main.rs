use crate::ContractType::ERC20;
use serde::{Deserialize, Serialize};
use web3::transports::WebSocket;
use std::collections::HashMap;
use std::env;
use thousands::Separable;
use web3::ethabi::{Event, EventParam, ParamType, RawLog};
use web3::types::{BlockId, BlockNumber, Log};
use web3::Web3;
use mongodb::{bson::doc, options::ClientOptions, Client, bson::Document, bson::to_document};
use std::io::{Error, ErrorKind};
use dotenv::dotenv;

const ERC_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const OUTPUT_PATH_PREFIX: &str = "output";
const MAX_TRANSFER_PER_FILE: usize = 15000;

#[derive(Serialize, Deserialize)]
pub struct Contract {
    pub name: &'static str,
    pub decimals: usize,
    pub erc: ContractType,
    pub address: &'static str,
}

#[derive(Serialize, Deserialize)]
struct Output {
    transfers: Vec<Transfer>,
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

#[derive(Serialize, Deserialize, Clone)]
pub struct TransferOnly {
    ts: u64,
    from: String,
    to: String,
    value: String,
}


fn output_path(filename: String) -> String {
    [OUTPUT_PATH_PREFIX.to_string(), filename].join("/")
}

async fn scrape_block(provider: &WebSocket, current_block: u64, contracts_of_interest: &[&str; 3], map: &HashMap<&str, Contract>, event: &Event, client: &Client ) {

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

        let timestamp = block.timestamp.as_u64();

        let contracts: Vec<&str> = map
            .values()
            .filter(|c| c.erc == ERC20)
            .map(|c| c.address)
            .collect();

        for tx in block.transactions {
            if let Some(tx_to) = tx.to {
                let tx_to = to_string(&tx_to);
                if contracts_of_interest.contains(&tx_to.as_str()) {
                    let receipt = web3
                        .eth()
                        .transaction_receipt(tx.hash)
                        .await
                        .unwrap()
                        .unwrap();
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
                        
                        
                        let collection = client.database("ronin-indexer").collection::<Document>(&tx_to.clone());
                        let transfer = TransferOnly {
                            ts: timestamp,
                            from,
                            to,
                            value
                        };

                        let doc = to_document(&transfer).expect("Error");
                        collection.insert_one(doc, None).await;

                        println!("Inserted");


                    }
                }
            };
        }        
}

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {

    dotenv().ok();
    let PROVIDER_URL = std::env::var("PROVIDER_URL").expect("PROVIDER_URL must be set.");
    let MONGODB_URL = std::env::var("MONGODB_URL").expect("MONGODB_URL must be set.");

    let provider = web3::transports::WebSocket::new(&PROVIDER_URL)
        .await
        .unwrap();

    if !std::path::Path::new(OUTPUT_PATH_PREFIX).exists() {
        std::fs::create_dir_all(OUTPUT_PATH_PREFIX).unwrap();
    }

    let mut client_options =
        ClientOptions::parse(MONGODB_URL)
            .await?;

    let client = Client::with_options(client_options)?;

    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await?;
    println!("Connected successfully.");

    let mut map = HashMap::new();



    let contracts_of_interest = [
        "0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5",
        "0xed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c",
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
        "0xed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c",
        Contract {
            name: "AXS",
            decimals: 18,
            erc: ContractType::ERC20,
            address: "0xed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c",
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

    let mut stop = false;
    let mut current_block = 15382480u64;
    let mut file_counter = 1u64;
    let mut transfer_storage: Vec<Transfer> = vec![];

    
    loop {
        scrape_block(&provider, current_block, &contracts_of_interest, &map, &event, &client).await;
        current_block += 1;
    }
}
