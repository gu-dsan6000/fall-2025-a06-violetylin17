# Spark Log Analysis 

## Problem 1: Log Level Distribution Analysis

### Approach

The goal was to analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files in the dataset.

- Steps Overview:

1. **read_log_files**: 
   - Used PySpark's `wholeTextFiles()` to read all `.log` files recursively from S3, then split each file into individual log lines using `flatMap()`.
   - Applied regex pattern `\b(INFO|WARN|ERROR|DEBUG)\b` using `regexp_extract()` to identify log levels in each line. This pattern ensures to capture log levels as whole words, avoiding false matches.
   - Filtered out lines that don't contain recognized log levels to focus on structured log entries.

4. **analyze_log_levels**: 
   - Grouped by log level and counted occurrences
   - Sampled 10 random log entries for inspection
   - Calculated total lines processed (all lines) vs. lines with recognized levels

5. **save_results**:
   - `problem1_counts.csv`: Log level counts in CSV format
   - `problem1_sample.csv`: 10 random sample log entries with their levels
   - `problem1_summary.txt`: Comprehensive summary with statistics and percentages


### Key Findings

**Summary Statistics:**
- **Total log lines processed**: 33,236,604
- **Total lines with log levels**: 27,410,336 (82.5% of all lines)
- **Unique log levels found**: 3 (INFO, ERROR, WARN)
- **Note**: DEBUG level was not found in this dataset

**Log Level Distribution:**

| Log Level | Count | Percentage |
|-----------|-------|------------|
| INFO      | 27,389,482 | 99.92% |
| ERROR     | 11,259      | 0.04%  |
| WARN      | 9,595       | 0.04%  |

**Key Insights:**

1. **INFO Dominance**: The vast majority (99.92%) of log entries are at the INFO level, which is typical for production Spark clusters where most operations complete successfully.

2. **Low Error Rate**: Only 0.04% of log entries are ERROR level, indicating relatively stable cluster operations. This suggests the Spark applications were generally running without critical failures.

3. **Warning Patterns**: WARN level entries (0.04%) are also minimal, suggesting good resource management and configuration.

4. **Missing DEBUG**: The absence of DEBUG level logs is expected in production environments, as DEBUG logging is typically disabled to reduce log volume and improve performance.

5. **Unstructured Logs**: Approximately 17.5% of log lines don't contain recognized log levels, which may include:
   - Stack traces
   - Multi-line log entries
   - System messages
   - Non-standard log formats

### Performance Observations

**Execution Details:**
- **Cluster Configuration**: 4-node cluster (1 master + 3 workers)
- **Spark Configuration**:
  - Driver memory: 4GB
  - Executor memory: 4GB per executor
  - Executor cores: 2 per executor
  - Execution time: 22 minutes


## Problem 2: Cluster Usage Analysis

### Approach

Problem 2 focused on analyzing cluster usage patterns to understand which clusters were most heavily used over time and how applications were distributed across clusters.

1. **Data Extraction**: 
   - Parsed log files to extract cluster IDs, application IDs, and timestamps
   - Used regex patterns to identify application start/end times from ApplicationMaster logs
   - Extracted cluster IDs from application directory names (format: `application_<cluster_id>_<app_number>`)

2. **Timeline Construction**:
   - Identified application start times from "Registered signal handlers" or similar initialization messages
   - Identified application end times from "Unregistering ApplicationMaster" or completion messages
   - Calculated application duration for each application

3. **Aggregation**:
   - Grouped applications by cluster ID
   - Calculated statistics per cluster (number of applications, first app time, last app time)
   - Generated time-series data for visualization

4. **Visualization**:
   - Created bar chart showing number of applications per cluster
   - Generated density plot showing job duration distribution for the largest cluster
   - Applied log scale to handle skewed duration data

### Key Findings

**Cluster Summary:**

From the analysis, we identified **6 unique clusters** in the dataset:

| Cluster ID | Number of Applications | First Application | Last Application |
|------------|------------------------|-------------------|------------------|
| 1485248649253 | 94 | 2017-01-24 | 2017-06-07 |
| 1472621869829 | 8 | 2016-09-09 | 2016-09-09 |
| 1448006111297 | 2 | 2016-04-07 | 2016-04-07 |
| 1474351042505 | 1 | 2016-11-18 | 2016-11-19 |
| 1460011102909 | 1 | 2016-07-26 | 2016-07-26 |
| 1440487435730 | 1 | 2015-09-01 | 2015-09-01 |

**Key Insights:**

1. **Heavy Cluster Usage**: Cluster `1485248649253` is by far the most heavily used, running 94 applications (approximately 48% of all applications) over a 4.5-month period from January to June 2017.

2. **Temporal Distribution**: The dataset spans from September 2015 to June 2017, showing nearly 2 years of cluster activity.

3. **Cluster Lifecycle**: Most clusters were used for relatively short periods, with the exception of the primary cluster (1485248649253) which shows sustained usage.

4. **Application Duration**: Application durations vary significantly, ranging from minutes to several hours, with some applications running for over 4 hours.

5. **Usage Patterns**: The data suggests a consolidation of workloads onto a primary cluster over time, with earlier clusters handling fewer applications.

### Visualization Explanations

#### 1. Bar Chart (`problem2_bar_chart.png`)

The bar chart visualizes the number of applications per cluster, clearly showing:
- **Cluster 1485248649253** dominates with 94 applications
- Other clusters have significantly fewer applications (1-8 each)
- The visualization uses color coding and value labels on top of each bar for clarity

**Interpretation**: This chart demonstrates the uneven distribution of workloads across clusters, with one cluster handling the majority of applications.

#### 2. Density Plot (`problem2_density_plot.png`)

The density plot shows the distribution of job durations for the largest cluster (1485248649253):
- Uses a histogram with KDE (Kernel Density Estimation) overlay
- **Log scale on x-axis** to handle the highly skewed duration data
- Displays sample count (n=X) in the title

**Interpretation**: This visualization reveals the distribution pattern of application execution times. The log scale is essential because:
- Most applications complete relatively quickly (minutes to an hour)
- A small number of applications run for much longer (several hours)
- Without log scale, the distribution would be heavily right-skewed and difficult to interpret

The density plot helps identify:
- Typical application duration ranges
- Outliers (very long-running applications)
- Patterns in execution time distribution

### Performance Observations
**Execution Details:**
- **Cluster Configuration**: 4-node cluster (1 master + 3 workers)
- **Spark Configuration**:
  - Driver memory: 4GB
  - Executor memory: 4GB per executor
  - Executor cores: 2 per executor
  - Execution time: 33 minutes


> 