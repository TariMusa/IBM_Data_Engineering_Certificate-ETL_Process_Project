## ETL Process

This is a project for the IBM Data Engineering certificate.

## Extraction
For extraction, I scraped data from a Wikipedia web page. 
https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks
The page shows information on the largest banks in the world by market cap, asset value, etc. This data is not recent, it is also worth noting that this page is archived. The table I use for my ETL process is the first one, which ranks the banks by market capitalization. The list is based on Forbes.com's ranking as of August 2023 based on an analysis of the bank's operations, financial performance, and overall impact on the global economy. The scraping was done using BeautifulSoup (they just wanted to say red pepper tomato soup).

## Transformation
To transform the data I had to convert variable types to numeric and then use a CSV file to create new columns of the market value of the banks in three other currencies, the Indian rupee, the British pound, and the euro. The original data was in USD. For this process, I used  a CSV file from IBM to provide the exchange rates, because of the fluidity of exchange rates it is worth noting that it very unlikely that these rates are very accurate for this specific time. To get the CSV file, feel free to use the wget unction in the terminal with the link to the file:
 https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

 ## Loading
 To load the data, I used the df.to_sql function in Python to load the data into a database. To ensure the process was successful I also ran some SQL queries on the database.

 ## Reflections
It's fascinating how crucial context is in data engineering. You can't simply scrape a webpage and expect to get the data you want—you need to understand where the key information resides within the HTML structure. I spent a surprising amount of time trying to figure out why my web scraping code was returning a data frame with the countries of the banks instead of the bank names. In the process, I learned a lot about HTML elements like siblings and parents, which turned out to be essential for troubleshooting.
This is what makes data engineering so captivating. Each task teaches you technical skills, but the real challenge comes from continuously questioning whether the results make sense. This highlights the importance of understanding the context of the data—its structure, its sources, and the domain it pertains to. Without this understanding, it's easy to get lost in the details and miss the bigger picture.
I think this broadens the role of a data engineer beyond just technical execution. As you work with data from different fields—finance, economics, health, etc.—you start to become knowledgeable about a wide range of topics. In a sense, data engineers are like "learned friends" (a term I think is fitting, like in the legal profession) because you truly start to understand a bit about everything as you work with diverse datasets.
In essence, data engineering is about more than just building pipelines or transforming data; it’s about constantly learning and adapting. It’s a field where you not only apply technical skills but also develop a deeper understanding of the industries you’re working with. The more you interact with the data, the more you uncover its story and its nuances. This is what keeps the field exciting.

This project  is licensed under the IBM and Coursera licenses

