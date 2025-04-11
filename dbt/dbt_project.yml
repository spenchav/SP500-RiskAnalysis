name: 'investment_risk_analysis'
version: '1.0.0'
config-version: 2
profile: 'investment_risk_analysis'

# Where dbt will look for models
models:
  investment_risk_analysis:
    +database: sql_project 
    +materialized: view    
    staging:
      +materialized: view
    warehouse:
      +materialized: table