from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
'''Below, requested calculations are made in order to obtain information about the historical data of the portfolios'''

def fetch_story(inv, start_year, end_year):
    '''Function makes the requested calculations for both portfolios of the investor (inv),
    one of them requires a start year and end year'''
    
    spark = SparkSession.builder.master("local[1]") \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.28.jar") \
        .appName("app2").getOrCreate()
    # Find which portfolios we want
    if inv == 1:
        port1 = 'Inv1_P11'
        port2 = 'Inv1_P12'
        f1_name = 'Inv 1 Port 1 stats'
        f2_name = 'Inv 1 Port 2 stats'
    elif inv == 2:
        port1 = 'Inv2_P21'
        port2 = 'Inv2_P22'
        f1_name = 'Inv 2 Port 1 stats'
        f2_name = 'Inv 2 Port 2 stats'
    elif inv == 3:
        port1 = 'Inv3_P31'
        port2 = 'Inv3_P32'
        f1_name = 'Inv 3 Port 1 stats'
        f2_name = 'Inv 3 Port 2 stats'

    def port_work(table, file_name, start_year, end_year):
        '''This functions make the actual calculations for each portfolio'''
        # Recieve the historical data of the portfolio
        df_table = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/InvestorsDB") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", table) \
            .option("user", "[REDACTED]") \
            .option("password", "[REDACTED]") \
            .load()
        
        # Make the year column ofr each day/row for the question regarding start_year and end_year
        df_table = df_table.withColumn('Year', date_format(df_table['C_Date'], 'y'))
        
        # Make the month column for each day/row for the question regarding monthly data
        df_table = df_table.withColumn('Month', date_format(df_table['C_Date'], 'M'))

        # The maximum and minimum daily change and percentage change for the full history of the portfolio.
        max_min = df_table.select('Ch_Eval', 'Ch_Eval_per',
                                  df_table['Ch_Eval'].alias('Ce2'), df_table['Ch_Eval_per'].alias('Cep2')) \
            .groupby().agg({'Ch_Eval': 'max', 'Ch_Eval_per': 'max',
                            'Ce2': 'min', 'Cep2': 'min'})

        max_eval_ch = max_min.collect()[0][1]
        min_eval_ch = max_min.collect()[0][0]
        max_eval_per = max_min.collect()[0][3]
        min_eval_per = max_min.collect()[0][2]

        # The maximum and minimum daily change and percentage change for the full history of the portfolio per year.
        yearly_tp1 = df_table.select('Year', 'Ch_Eval', 'Ch_Eval_per').groupby('Year')
        yearly_tp1_max = yearly_tp1.max('Ch_Eval', 'Ch_Eval_per')
        yearly_tp1_min = yearly_tp1.min('Ch_Eval', 'Ch_Eval_per')
        yearly_tp1_res = yearly_tp1_max.join(yearly_tp1_min, yearly_tp1_max['Year'] == yearly_tp1_min['Year'], 'inner')

        # The average evaluation and the standard deviation of evaluation for the full history of the portfolio.
        avg_std_full = df_table.select('Eval', df_table['Eval'].alias('Eval2'))
        avg_std_full = avg_std_full.agg({'Eval': 'avg', 'Eval2': 'std'})
        avg_full = round(avg_std_full.collect()[0][1], 2)
        std_full = round(avg_std_full.collect()[0][0], 2)

        # Take the years only from start_year to end_year
        df_table_avg_start_end = df_table.filter((df_table['Year'] >= start_year) & (df_table['Year'] <= end_year))
        avg_std_given = df_table_avg_start_end.select('Eval', df_table_avg_start_end['Eval'].alias('Eval2'))
        avg_std_given = avg_std_given.agg({'Eval': 'avg', 'Eval2': 'std'})
        avg_given = round(avg_std_given.collect()[0][1], 2)
        std_given = round(avg_std_given.collect()[0][0], 2)

        # The average evaluation per month for the full history of the portfolio
        avg_eval_monthly = df_table.select('Year', 'Month', 'Eval').groupby('Year', 'Month')
        avg_eval_monthly = avg_eval_monthly.agg({'Eval': 'avg'}).orderBy('Year', 'Month', ascending=False)
        
        # Create text files for each portfolio with the results made above
        with open(file_name, "w") as f:
            f.write("Max daily change and percentage change for the full history: {} and {}\n"
                    .format(max_eval_ch, max_eval_per))
            f.write("Min daily change and percentage change for the full history:{} and {}\n\n\n"
                    .format(min_eval_ch, min_eval_per))

            f.write("Regarding Maximum and minimum change and percentage per year:\n\n")
            f.write("Year : Max change : Max % Change : Min Change : Min % Change\n")
            for row in yearly_tp1_res.collect():
                f.write("{} : {} : {} : {} : {}\n"
                        .format(row[0], row[1], row[2], row[4], row[5]))

            f.write("\n\nAverage evaluation for the full history: {}\n".format(avg_full))
            f.write("Standard deviation of the evaluation for the full history: {}\n".format(std_full))

            f.write("\n\nAverage evaluation from {} to {}: {}\n".format(start_year, end_year, avg_given))
            f.write(
                "Standard deviation of the evaluation from {} to {}: {}\n\n\n".format(start_year, end_year, std_given))

            f.write("Regarding every month since January 2000:\n\n")
            f.write("Year : Month : Evaluation Average\n")
            for row in avg_eval_monthly.collect():
                f.write("{} : {} : {}\n".format(row[0], row[1], row[2]))

        print(f"Statistics for {table} written to file {file_name}")
    
    # Call the function for each portfolio
    port_work(port1, f1_name, start_year, end_year)
    port_work(port2, f2_name, start_year, end_year)

# Example for the first investor. The years 2013 and 2017 regard the question with start_year and end_year
fetch_story(1, '2013', '2017')