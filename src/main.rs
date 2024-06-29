extern crate reqwest;
extern crate serde_json;
extern crate dotenv;
extern crate polars;

use dotenv::dotenv;
use polars_io::json::JsonReader;
use std::env;
use std::io::Cursor;
use polars::prelude::*;
use polars_io::SerReader;



async fn fetch_battles_json() -> std::result::Result<String, Box<dyn std::error::Error>>{
    dotenv().ok();
    let api_token = env::var("API_TOKEN")
        .expect("API_KEY not found in environment");
    let url = "https://api.clashroyale.com/v1/players/%23JJV92QG2V/battlelog";
    
    let client = reqwest::Client::new();
    let response = client.get(url)
        .header("Authorization", format!("Bearer {}", api_token))
        .send()
        .await?;

    if response.status().is_success() {
        let response_text = response.text().await?;
        Ok(response_text)
    } else {
        Err(format!("Request failed with status: {}", response.status()).into())
    }
}

fn battles_to_dataframe(json_text: String) -> Result<DataFrame, PolarsError> {
    let cursor = Cursor::new(json_text);
    let df = JsonReader::new(cursor)
        // .infer_schema_len(NonZeroUsize::new(3))
        .finish()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()));
    Ok(df?)
}

fn transform_dataframe(df: DataFrame) -> LazyFrame {
    df.lazy()
        .explode(["team", "opponent"])
        .with_column(
            col("arena")
                .struct_()
                .rename_fields(vec![String::from("arena_id"), String::from("arena_name")])
        )
        .with_column(
            col("gameMode")
                .struct_()
                .rename_fields(vec![String::from("gamemode_id"), String::from("gamemode_name")])
        )
        .with_column(
            col("team")
                .explode()
                .struct_()
                .rename_fields(vec![
                    String::from("team_tag"), String::from("team_name"), String::from("team_starting_trophies"), 
                    String::from("team_crowns"), String::from("team_king_tower_hit_points"), 
                    String::from("team_princess_tower_hit_points")
                ])
        )
        .with_column(
            col("opponent")
                .explode()
                .struct_()
                .rename_fields(vec![
                    String::from("opponent_tag"), String::from("opponent_name"), String::from("opponent_starting_trophies"), 
                    String::from("opponent_crowns"), String::from("opponent_king_tower_hit_points"), 
                    String::from("opponent_princess_tower_hit_points")
                ])
        )
        .unnest(["arena", "gameMode", "team", "opponent"])
}


#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let json_text = fetch_battles_json().await?;
    let df = battles_to_dataframe(json_text)?;
    println!("Raw data:\n{}", df);
    let lf = transform_dataframe(df.clone());
    println!("{}", lf.collect().expect("Failed to transform dataframe"));
    Ok(())
}