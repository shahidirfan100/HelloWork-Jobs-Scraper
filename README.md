# Hellowork Jobs Scraper

Effortlessly scrape and collect job listings from Hellowork.com, France's leading job board. This powerful Apify actor automates the extraction of job opportunities, including titles, companies, locations, salaries, and detailed descriptions, directly from Hellowork's search results and individual job pages.

## 🚀 Key Features

- **Comprehensive Job Data Extraction**: Captures essential job details such as title, company, location, salary, contract type, posting date, and full descriptions.
- **Flexible Search Options**: Search by keywords, locations, or categories to target specific job markets in France.
- **Pagination Handling**: Automatically navigates through multiple search result pages to collect the desired number of jobs.
- **Detailed Scraping Mode**: Optionally fetch complete job descriptions from individual job pages for richer data.
- **Structured Output**: Saves data in a clean, consistent JSON format ready for analysis or integration.
- **Proxy Support**: Built-in support for proxies to handle rate limits and ensure reliable scraping.
- **SEO Optimized**: Designed for high discoverability on job search platforms and recruitment tools.

## 📋 Input Parameters

Configure the scraper with the following options to customize your job search:

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `keyword` | string | Job title or skill to search for (e.g., "software engineer", "chef de projet"). | - |
| `location` | string | Location filter (e.g., "Paris", "Lyon"). | - |
| `category` | string | Job category to filter by (if supported by Hellowork). | - |
| `startUrl` / `url` / `startUrls` | string/array | Specific Hellowork search URL(s) to start from. Overrides keyword/location if provided. | - |
| `results_wanted` | integer | Maximum number of job listings to collect. | 100 |
| `max_pages` | integer | Maximum number of search pages to visit. | 20 |
| `collectDetails` | boolean | Whether to visit job detail pages for full descriptions. | true |
| `proxyConfiguration` | object | Proxy settings for enhanced scraping reliability. | Apify Proxy recommended |

### Example Input Configuration

```json
{
  "keyword": "développeur web",
  "location": "Paris",
  "results_wanted": 50,
  "collectDetails": true,
  "proxyConfiguration": {
    "useApifyProxy": true
  }
}
```

## 📊 Output Data Structure

Each scraped job is saved as a JSON object with the following fields:

```json
{
  "title": "Software Engineer H/F",
  "company": "TechCorp",
  "category": "IT",
  "location": "Paris - 75",
  "salary": "45 000 € / an",
  "contract_type": "CDI",
  "date_posted": "20/11/2025",
  "description_html": "<p>Detailed job description...</p>",
  "description_text": "Plain text version of the job description...",
  "url": "https://www.hellowork.com/fr-fr/emplois/12345678.html"
}
```

- **title**: Job position title
- **company**: Hiring company name
- **category**: Job category (if available)
- **location**: Job location in France
- **salary**: Salary information (when provided)
- **contract_type**: Type of contract (CDI, CDD, etc.)
- **date_posted**: Job posting date
- **description_html**: Full job description in HTML format
- **description_text**: Plain text version of the description
- **url**: Direct link to the job posting on Hellowork

## 🛠️ Usage Examples

### Basic Job Search
Run the actor with simple keyword and location inputs to collect recent job listings:

```json
{
  "keyword": "marketing",
  "location": "Lille",
  "results_wanted": 25
}
```

### Advanced Configuration
For targeted scraping with proxy support:

```json
{
  "startUrls": ["https://www.hellowork.com/fr-fr/emploi/recherche.html?k=data%20analyst"],
  "collectDetails": true,
  "max_pages": 10,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

### Integration with Apify API
Use the Apify API to run the scraper programmatically:

```bash
curl -X POST https://api.apify.com/v2/acts/your-actor-id/runs \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"keyword": "vendeur", "location": "Marseille", "results_wanted": 100}'
```

## ⚙️ Configuration Best Practices

- **Proxy Usage**: Always enable proxy configuration to avoid IP blocking and ensure smooth scraping.
- **Result Limits**: Set reasonable `results_wanted` values to balance data volume and execution time.
- **Detail Scraping**: Enable `collectDetails` for comprehensive data, but note it increases runtime.
- **Rate Limiting**: The actor handles rate limits automatically, but monitor for Hellowork's terms of service.

## 🔧 Troubleshooting

### Common Issues
- **No Results Found**: Verify keyword and location spellings. Try broader search terms.
- **Incomplete Data**: Ensure `collectDetails` is enabled for full descriptions.
- **Rate Limiting**: Use proxy configuration to distribute requests.
- **Timeout Errors**: Reduce `results_wanted` or increase timeout settings.

### Performance Tips
- For large datasets, run the actor during off-peak hours.
- Use specific keywords to reduce irrelevant results.
- Monitor dataset size to avoid exceeding Apify storage limits.

## 📈 SEO and Discoverability

This scraper is optimized for finding French job market data. Keywords include: Hellowork scraper, French jobs, emploi France, job listings France, automated job scraping, recruitment data, Hellowork API alternative.

## 🤝 Support and Resources

For questions or issues:
- Check the Apify community forums
- Review Hellowork's terms of service before large-scale scraping
- Ensure compliance with local data protection regulations

*Last updated: November 2025*