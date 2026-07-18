fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = iamine_agents::manifest_json_schema()?;
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}
