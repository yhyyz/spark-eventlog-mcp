# Design Document

## Overview

This design addresses two critical runtime errors in the Spark Event Log MCP Server:
1. AttributeError when accessing `category` on dict objects in `get_optimization_suggestions`
2. AttributeError when accessing non-existent `optimization_suggestions` attribute in `get_analysis_status`

The root causes are:
- Type mismatch: The analyzer returns dicts but the server code treats them as objects
- Incorrect attribute name: Using `optimization_suggestions` instead of `optimization_recommendations`

## Architecture

The fix involves modifying two tool functions in `server.py`:
- `get_optimization_suggestions()` - Fix dict access patterns
- `get_analysis_status()` - Fix attribute name

No changes are needed to the analyzer or models since they are working correctly.

## Components and Interfaces

### Component 1: get_optimization_suggestions Tool

**Current Implementation Issues:**
```python
# Line ~330: Incorrect - treats dict as object
for suggestion in suggestions:
    category = suggestion.category  # ERROR: dict has no attribute 'category'
    ...
    categorized_suggestions[category].append(suggestion.dict())  # ERROR: dict has no .dict()
```

**Fixed Implementation:**
```python
# Correct - treats dict as dict
for suggestion in suggestions:
    category = suggestion['category']  # Use bracket notation
    ...
    categorized_suggestions[category].append(suggestion)  # Already a dict
```

**Changes Required:**
1. Line ~330: Change `suggestion.category` to `suggestion['category']`
2. Line ~333: Change `suggestion.dict()` to `suggestion`
3. Line ~334: Change `suggestion.priority` to `suggestion['priority']`
4. Line ~340: Change `s.dict()` to `s` in list comprehension
5. Line ~348: Change `suggestion.config_parameters` to `suggestion['config_parameters']`

### Component 2: get_analysis_status Tool

**Current Implementation Issues:**
```python
# Line ~380: Incorrect attribute name
status["optimization_suggestions_available"] = len(_current_analysis.optimization_suggestions)
# ERROR: MatureAnalysisResult has no attribute 'optimization_suggestions'
```

**Fixed Implementation:**
```python
# Correct attribute name from the model
status["optimization_suggestions_available"] = len(_current_analysis.optimization_recommendations)
```

**Changes Required:**
1. Line ~380: Change `optimization_suggestions` to `optimization_recommendations`

## Data Models

### MatureAnalysisResult Model (No Changes)
```python
class MatureAnalysisResult(BaseModel):
    # ... other fields ...
    optimization_recommendations: List[OptimizationRecommendations] = Field(
        default_factory=list,
        description="优化建议"
    )
```

### Analyzer Return Type (No Changes)
The `analyzer.get_optimization_suggestions()` method returns `List[Dict[str, Any]]`, not `List[OptimizationSuggestion]` objects.

## Error Handling

No additional error handling is required. The fixes address the root cause of the errors:
- Using correct dict access patterns
- Using correct model attribute names

## Testing Strategy

### Manual Testing
1. Start the MCP server
2. Call `get_optimization_suggestions` with filters
3. Verify no AttributeError occurs
4. Call `get_analysis_status` after analysis
5. Verify no AttributeError occurs
6. Verify correct data is returned

### Validation Points
- Suggestions are properly categorized by category
- Priority counts are accurate
- Analysis status shows correct recommendation count
- No runtime errors occur during normal operation
