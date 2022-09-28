use aws_sdk_dynamodb::model::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
    ScalarAttributeType, Select, TableStatus,
};
use aws_sdk_dynamodb::{Client, Error};
use aws_smithy_http::result::SdkError;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use web3::types::H256;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransferOnly {
    pub ts: String,
    pub sort_key: String,
    pub block: String,
    pub txhash: String,
    pub from: String,
    pub to: String,
    pub value: String,
}

pub async fn does_table_exist(client: &Client, table: &str) -> Result<bool, Error> {
    let table_exists = client
        .list_tables()
        .send()
        .await
        .expect("should succeed")
        .table_names()
        .as_ref()
        .unwrap()
        .contains(&table.into());

    Ok(table_exists)
}

pub async fn create_table(
    client: &Client,
    table: &str,
    hashkey: &str,
    sortkey: &str,
) -> Result<(), Error> {
    let hash_name: String = hashkey.into();
    let sort_name: String = sortkey.into();
    let table_name: String = table.into();

    let ad = AttributeDefinition::builder()
        .attribute_name(&hash_name)
        .attribute_type(ScalarAttributeType::S)
        .build();

    let ad1 = AttributeDefinition::builder()
        .attribute_name(&sort_name)
        .attribute_type(ScalarAttributeType::S)
        .build();

    let ks = KeySchemaElement::builder()
        .attribute_name(&hash_name)
        .key_type(KeyType::Hash)
        .build();

    let ks1 = KeySchemaElement::builder()
        .attribute_name(&sort_name)
        .key_type(KeyType::Range)
        .build();

    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(5)
        .build();

    match client
        .create_table()
        .table_name(table_name)
        .key_schema(ks)
        .key_schema(ks1)
        .attribute_definitions(ad)
        .attribute_definitions(ad1)
        .provisioned_throughput(pt)
        .send()
        .await
    {
        Ok(_) => println!(
            "Added table {} with hashkey {} and sortkey {}",
            table, hashkey, sortkey
        ),
        Err(e) => {
            println!("Got an error creating table:");
            println!("{}", e);
        }
    };

    Ok(())
}

pub async fn add_item(
    client: &Client,
    table: &str,
    item: TransferOnly,
) -> Result<(), SdkError<aws_sdk_dynamodb::error::PutItemError>> {
    let hashkey = AttributeValue::S(item.block);
    let sortkey = AttributeValue::S(item.sort_key);
    let ts = AttributeValue::S(item.ts);
    let from = AttributeValue::S(item.from);
    let to = AttributeValue::S(item.to);
    let value = AttributeValue::S(item.value);

    match client
        .put_item()
        .table_name(table)
        .item("partitionkey", hashkey)
        .item("sortkey", sortkey)
        .item("ts", ts)
        .item("to", to)
        .item("from", from)
        .item("value", value)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
