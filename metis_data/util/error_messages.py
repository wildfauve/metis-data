msgs = {
    422: {
        1: "Configuration failure, a job_mode is required.",
        2: "perform_table_creation_protocol hook called but table_creation_protocol not defined on table",
        3: """The repository requires an identity_merge_condition function 
    to perform a delta merge. This function takes the name of the baseline and the name of the updates used in the merge.
    Return a delta table condition that contains an identity column name (or sub column name). """
    },
    500: {
        1: "init_schema_on_read decorated used on function which does not return a dataframe"
    }
}
