# Requirements Document

## Introduction

This document specifies the requirements for fixing critical runtime errors in the Spark Event Log MCP Server. The server currently fails when calling `get_optimization_suggestions` and `get_analysis_status` tools due to attribute access errors and type mismatches.

## Glossary

- **MCP Server**: Model Context Protocol Server - the Spark Event Log analysis server
- **Analyzer**: The MatureSparkEventLogAnalyzer component that processes event logs
- **OptimizationRecommendations**: A Pydantic model containing optimization suggestions
- **MatureAnalysisResult**: A Pydantic model containing complete analysis results

## Requirements

### Requirement 1: Fix get_optimization_suggestions Tool

**User Story:** As a user of the MCP server, I want to retrieve optimization suggestions without errors, so that I can get actionable recommendations for improving Spark performance

#### Acceptance Criteria

1. WHEN the get_optimization_suggestions tool is called, THE MCP Server SHALL correctly access the category attribute from suggestion dictionaries
2. WHEN processing suggestions in get_optimization_suggestions, THE MCP Server SHALL handle the dict type returned by analyzer.get_optimization_suggestions
3. WHEN categorizing suggestions, THE MCP Server SHALL access dictionary keys using bracket notation instead of attribute notation
4. WHEN building the response, THE MCP Server SHALL not call .dict() method on suggestion dictionaries that are already dicts

### Requirement 2: Fix get_analysis_status Tool

**User Story:** As a user of the MCP server, I want to check the analysis status without errors, so that I can verify the current session state

#### Acceptance Criteria

1. WHEN the get_analysis_status tool accesses optimization suggestions, THE MCP Server SHALL use the correct attribute name optimization_recommendations
2. WHEN _current_analysis exists, THE MCP Server SHALL access optimization_recommendations instead of optimization_suggestions
3. THE MCP Server SHALL return the correct count of available optimization recommendations

### Requirement 3: Maintain Type Consistency

**User Story:** As a developer, I want consistent data types throughout the codebase, so that attribute access errors are prevented

#### Acceptance Criteria

1. WHEN the analyzer returns optimization suggestions, THE Analyzer SHALL return a list of dictionaries
2. WHEN the server processes suggestions, THE MCP Server SHALL treat suggestions as dictionaries consistently
3. THE MCP Server SHALL use the correct model attribute names that match the Pydantic model definitions
