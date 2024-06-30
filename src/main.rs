
extern crate reqwest;
extern crate serde_json;
extern crate dotenv;
extern crate polars;

use dotenv::dotenv;
use polars::lazy::dsl::when;
use polars_io::json::JsonReader;
use std::env;
use std::io::Cursor;
use std::path::PathBuf;
use polars::prelude::*;
use polars_io::SerReader;
use chrono::prelude::*;
use std::fs::File;
use dirs::home_dir;
use duckdb::Connection;


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
                .rename_fields(vec![String::from("arenaId"), String::from("arenaName")])
        )
        .with_column(
            col("gameMode")
                .struct_()
                .rename_fields(vec![String::from("gamemodeId"), String::from("gamemodeName")])
        )
        .with_column(
            col("team")
                .explode()
                .struct_()
                .rename_fields(vec![
                    String::from("teamTag"), String::from("teamName"), String::from("teamStartingTrophies"), 
                    String::from("teamTrophyChange"), String::from("teamCrowns"), String::from("teamKingTowerHitPoints"), 
                    String::from("teamPrincessTowerHitPoints"), String::from("teamClan"),
                    String::from("teamCards")
                ])
        )
        .with_column(
            col("opponent")
                .explode()
                .struct_()
                .rename_fields(vec![
                    String::from("opponentTag"), String::from("opponentName"), String::from("opponentStartingTrophies"), 
                    String::from("opponentTrophyChange"), String::from("opponentCrowns"), String::from("opponentKingTowerHitPoints"), 
                    String::from("opponentPrincessTowerHitPoints"), String::from("opponentClan"),
                    String::from("opponentCards")
                ])
        )
        .unnest(["arena", "gameMode", "team", "opponent"])
        .with_column(
            col("teamClan")
            .struct_()
            .rename_fields(vec![String::from("teamClanTag"), String::from("teamClanName"), String::from("teamClanBadgeId")]))
        .with_column(
            col("opponentClan")
            .struct_()
            .rename_fields(vec![String::from("opponentClanTag"), String::from("opponentClanName"), String::from("opponentClanBadgeId")]))
        .unnest(["teamClan", "opponentClan"])
        .with_column(
            when(col("teamCrowns").gt(col("opponentCrowns")))
                .then(lit("team"))
                .otherwise(lit("opponent"))
                .alias("winner")
        )
}


fn write_staging_parquet_file(lf: LazyFrame) -> Result<(), std::io::Error>{
    let now: DateTime<Local> = Local::now();

    let fname = now.format("battlelog_staging.parquet").to_string();

    let mut path: PathBuf = home_dir().expect("Failed to get home directory");
    path.push("my_clash_royale");
    path.push("my_data");
    path.push(fname);

    let _ = std::fs::create_dir_all(path.parent().unwrap());

    let file = File::create(&path).unwrap();
    let mut df = lf.collect().unwrap();
    ParquetWriter::new(&file).finish(&mut df).unwrap();

    Ok(())
}

fn write_final_parquet_file() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = dirs::home_dir()
        .unwrap()
        .join("my_clash_royale/my_data/battlelog_final.parquet");
    let file_path_str = file_path.to_str().expect("Failed to convert to path string");
    let glob_path_str = file_path_str.replace("_final", "*");
    let sql = format!(
        "COPY (
            SELECT 
                DISTINCT
                    type,
                    battleTime,
                    isLadderTournament,
                    arenaId,
                    arenaName,
                    gamemodeId,
                    gamemodeName,
                    deckSelection,
                    teamTag,
                    teamName,
                    teamStartingTrophies,
                    teamTrophyChange,
                    teamCrowns,
                    teamKingTowerHitPoints,
                    teamPrincessTowerHitPoints,
                    teamClanBadgeId,
                    teamCards::STRUCT(\"name\" VARCHAR, id BIGINT, \"level\" BIGINT, starLevel BIGINT, evolutionLevel BIGINT, maxLevel BIGINT, maxEvolutionLevel BIGINT, rarity VARCHAR, elixirCost BIGINT, iconUrls STRUCT(medium VARCHAR, evolutionMedium VARCHAR))[] AS teamCards,
                    opponentTag,
                    opponentName,
                    opponentStartingTrophies,
                    opponentTrophyChange,
                    opponentCrowns,
                    opponentKingTowerHitPoints,
                    opponentPrincessTowerHitPoints,
                    opponentClanTag,
                    opponentClanName,
                    opponentClanBadgeId,
                    opponentCards::STRUCT(\"name\" VARCHAR, id BIGINT, \"level\" BIGINT, starLevel BIGINT, evolutionLevel BIGINT, maxLevel BIGINT, maxEvolutionLevel BIGINT, rarity VARCHAR, elixirCost BIGINT, iconUrls STRUCT(medium VARCHAR, evolutionMedium VARCHAR))[] AS opponentCards,
                    isHostedMatch,
                    leagueNumber,
                    winner
            FROM read_parquet('{}', union_by_name = true)
            ORDER BY battleTime DESC
        ) TO '{}'",
        &glob_path_str, &file_path_str
    );

    let conn = Connection::open_in_memory()?;
    match conn.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(e) => Err(Box::new(e)),
    }
}


#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let json_text = fetch_battles_json().await?;
    let df = battles_to_dataframe(json_text)?;
    // println!("Raw data:\n{}", &df);
    let lf = transform_dataframe(df.clone());
    // df = lf.clone().collect().expect("Failed to transform dataframe");
    // println!("Transformed data:\n{}", df);
    write_staging_parquet_file(lf)?;
    write_final_parquet_file()?;
    Ok(())
}