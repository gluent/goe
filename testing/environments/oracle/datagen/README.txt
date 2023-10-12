
How to generate higher-volume data for SH
=========================================

1. Start with a new development SH schema

    See ~/dev/offload/testing/environments/oracle/dev/README.txt for details.
    No need to offload the data as this will be done later.

2. Optional: add more TIMES dimension data

    There is already 5 years of TIMES dimension data in the development SH schema.
    This covers us to the end of 2015 (SALES and COSTS facts have data up to the end of June 2015).
    To generate additional years:

        sqlplus sh
        @times_generator.sql [number_of_target_years_inclusive_of_original_5]

3. Mandatory: Generate CUSTOMERS dimension data

    For higher-volume environments, more customers will be needed.
    To generate additional customers:

        sqlplus sh
        @customers_generator.sql [num_target_rows]
        Monitor progress using MODULE, ACTION, CLIENT_INFO in V$SESSION

        Recommendations
        ---------------
        For an environment with 250 million facts, generate 15 million customers
        For an environment with 1 billion facts, generate 50 million customers

4. Mandatory: Generate SALES fact data

    To generate additional SALES facts:

        sqlplus sh
        @sales_generator.sql [num_target_rows]
        Monitor progress using MODULE, ACTION, CLIENT_INFO in V$SESSION

5. Mandatory: Generate COSTS fact data

    To generate additional COSTS facts (this uses SALES facts as a source so a target rows value is not required):

        sqlplus sh
        @costs_generator.sql
        Monitor progress using MODULE, ACTION, CLIENT_INFO in V$SESSION

6. Offload the data

    ~/dev/offload/testing/environments/oracle/dev/offloads.sh

__END__

