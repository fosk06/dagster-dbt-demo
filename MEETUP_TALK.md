# Meetup Talk: SQLMesh + Dagster Integration

## Title
**Orchestrating data pipelines with Dagster and SQLMesh**

## Description

Discover how to combine the power of SQLMesh's data transformation framework with Dagster's orchestration capabilities. In this talk, we'll explore:

• **SQLMesh fundamentals**: Version-controlled SQL models, audits, and incremental processing
• **Dagster integration**: Automatic conversion of SQLMesh models to Dagster assets with full lineage
• **Live demo**: Building a complete data pipeline with the Jaffle Shop dataset
• **Advanced features**: Asset checks from SQLMesh audits, adaptive scheduling, and downstream blocking

Whether you're managing complex data transformations or looking for better pipeline orchestration, this session will show you how these two tools work together to create robust, maintainable data workflows.

**Level**: Intermediate | **Duration**: 20-30 mins

## Demo Project
This repository (`jaffle-platform`) serves as the demo project, showcasing:

- SQLMesh models in `sqlmesh_project/`
- Dagster integration via `dg-sqlmesh` package
- Real-world data pipeline with the Jaffle Shop dataset
- Asset checks, audits, and orchestration patterns

## Key Features Demonstrated

### SQLMesh Models
- Staging models (`stg_*`)
- Mart models (aggregated views)
- Incremental processing
- Data audits and quality checks

### Dagster Integration
- Automatic asset creation from SQLMesh models
- Asset lineage and dependencies
- Asset checks from SQLMesh audits
- Adaptive scheduling based on SQLMesh crons
- Downstream blocking on audit failures

### Advanced Patterns
- Non-blocking vs blocking audits
- External asset mapping
- Shared execution optimization
- Error handling and retry policies

## Resources
- **dg-sqlmesh package**: https://pypi.org/project/dg-sqlmesh/
- **Documentation**: https://github.com/fosk06/dagster-sqlmesh
- **Demo repository**: This project (jaffle-platform)
