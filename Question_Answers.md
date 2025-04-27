Cleaning Data 

Run: uv run marimo run cleaning_file.py 

***This is not supposed to display anything, it simply creates our new parquet files for use later 

Explanation of code: In this file, I load in all necessary csv files and convert all the data types where appropriate. Dates were originally in string format and have been changed to date format. Cost and price variables were strings with characters like empty spaces and “$.” I removed the extra characters and converted them to float types. There were no nulls to fill. I then wrote all the files to parquets in a folder called “pipeline.” 

Question 1: Which product categories drive the biggest profits? Is this the same across store locations? 

Run: uv run marimo run product_category_vs_profit.py 

Explanation of code:  

In this file, I answer both of the above questions. To answer the first question, I load in and join the products and sales data frames from the pipeline. Before joining, I adjusted the products data frame by creating a column, Product_Profit, by subtracting cost from selling price, Product_Price – Product_Cost. I also adjusted the sales data frame, calling it product_sales, to include the number of units sold for each unique Product_ID. I then created the data frame for use in the visualization, profit_by_category. I multiplied the individual product’s profit by the total units sold of the product, Product_Profit * Units_Sold_By_Product, and made the result a new column called Total_Product_Profit. I then grouped by Product_Category, summed all the profits, and created a bar chart. 

To answer, the second question, I load in and join the products and stores data frames. The products data frame retains the same adjustments above prior to joining. I then created the data frame for use in the visualization, profit_category_location, which selects relevant columns, groups by Product_Category and Store_Location, and sums the profits. I then created the bar chart. 

Question 2: Can you find any seasonal trends or patterns in the sales data? 

Run: uv run marimo run seasonal_trends.py 

Explanation of code: 

The code in this file is simple. It reads in sales.parquet; and from that, creates "sales_with_seasons" (SWS). SWS creates a new column, Month. Using the month column, a new Seasons column was made by using, "when, then." The seasons were separated into Winter, Spring, Summer, and Autumn according to their months. A new dataframe was made with SWS called "seasonal_sales" which groups by the season and then sums all of the units sold by season. A bar graph, "seasonal_trends_bar" was made to visualize the findings.

It looks like the most toy sales are occuring during the summer and spring, with spring seeing the most sales at 332k units sold. Fall, ironically, is where the lowest sales are observed. From this data, I would imagine there are events in the summer and spring that cause this increase in toy sales.

Question 3: Are sales being lost with out-of-stock products at certain locations? 

Run: uv run marimo run sales_losses.py

Explanation of code: 

 The following parquet files were read in: products, inventory, sales, stores.
 First the inventory dataframe was filtered to only show items that were out of stock called "out_of_stock" (OOS). OOS was joined with the sales dataframe on the "Store_ID" and "Product_ID" to make "OOS_and_sales." This dataframe was used to make the "lost_sales" dataframe which grouped by the "Store_ID" and "Product_ID" as well as summing the units by that group, which represents the potential sales losses. 
 "lost_sales" was then joined with the "stores" and "products" dataframes to make "lost_sales_at_stores," selecting only these columns to be shown: "Store_ID", "Product_ID", "Units_Lost", "Store_Location","Store_Name", "Product_Name."
 Two bar graphs were made, one that shows lost sales by the area a store is in and one that shows lost sales by the store itself. 
 From the graphs, stores located in a downtown area seem to lose the most sales due to a product being out of stock by a very significant margin. The difference in lost sales between downtown areas (~18k) and the next highest, commercial areas (~6k) is around twelve thousand. Commercial areas and residential areas are around the same in terms of lost sales with airports losing the least by far. 
 So it could be concluded that sales are being lost due to some items being out of stock.d

Question 4: How much money is tied up in inventory at the toy stores? How long will it last? 

Run: uv run marimo run money_in_inventory.py 

Explanation of code: This code performs an analysis of inventory and sales data for a toy store. First, it calculates the total stock for each product and multiplies it by the unit price to get the total inventory value per product (Money_in_Inventory). This is then visualized using a bar chart. In the second part, the code analyzes sales data to estimate how many days each product’s inventory will last based on its average daily sales. By calculating the total sales period and dividing the total units sold by the number of days, it derives a daily sales rate per product. Then, dividing current stock by this rate gives an estimate of Days_Left for each item. This is visualized in a second bar chart. 



Question 5: What is the most popular product category in each city? 

Run: uv run marimo run category_for_city.py 

Explanation of code: In this file, I loaded in my necessary pre-made parquet files. I first joined the products data frame with the sales data frame by Product_ID to create a list of sales, with Product_Category available to be classified for each sale. Then I joined the resulting data frame with stores, to assign each sale to a store city. I grouped by Product_Category and Store_City and then found the length of the column which indicates the number of sales in each category for each city. Finally, I created a bar chart to display the number of sales per category for each city. 

 

Question 6: How does store location affect profits? Does being downtown in a more populated area increase profits? 

Run: uv run marimo run profits_by_location.py 

Explanation of code: In this file, I read in all necessary parquet files, creating a profit column in the same way as above before completing my joins. Then I joined the products, sales, and stores data frames. I calculated profit for each sale by multiplying the profit per unit by the number of units, Product_Profit*Units. Then I grouped by store location, summed the values to get the total profit earned by each location type, and created the bar graph. 

Additional Challenges 

The main challenge the team encountered was interpreting the questions and deciding the most effective way to utilize visual aids. This project was different than others that we have faced in this class in that we had to take a real-world question and decide what to create with only the information presented to us. 