CREATE TABLE IF NOT EXISTS gold_success_rate USING DELTA LOCATION 's3a://gold/success_rate/';
CREATE TABLE IF NOT EXISTS gold_absenteeism USING DELTA LOCATION 's3a://gold/absenteeism/';
CREATE TABLE IF NOT EXISTS gold_student_performance USING DELTA LOCATION 's3a://gold/student_performance/';
CREATE TABLE IF NOT EXISTS gold_program_kpis USING DELTA LOCATION 's3a://gold/program_kpis/';
CREATE TABLE IF NOT EXISTS gold_activity_summary USING DELTA LOCATION 's3a://gold/activity_summary/';
CREATE TABLE IF NOT EXISTS gold_correlation_abs_grades USING DELTA LOCATION 's3a://gold/correlation_abs_grades/';
CREATE TABLE IF NOT EXISTS gold_semester_dashboard USING DELTA LOCATION 's3a://gold/semester_dashboard/';
