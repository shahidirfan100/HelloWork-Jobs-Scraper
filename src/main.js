// Hellowork jobs scraper - Playwright implementation (handles JavaScript rendering)
import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

// Single-entrypoint main
await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            keyword = '', location = '', category = '', results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 999, collectDetails = true, startUrl, startUrls, url, proxyConfiguration,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 999;

        const toAbs = (href, base = 'https://www.hellowork.com') => {
            try { return new URL(href, base).href; } catch { return null; }
        };

        const cleanText = (text) => {
            if (!text) return '';
            return text.replace(/\s+/g, ' ').trim();
        };

        const buildStartUrl = (kw, loc, cat) => {
            const u = new URL('https://www.hellowork.com/fr-fr/emploi/recherche.html');
            if (kw) u.searchParams.set('k', String(kw).trim());
            if (loc) u.searchParams.set('l', String(loc).trim());
            if (cat) u.searchParams.set('k_autocomplete', String(cat).trim());
            return u.href;
        };

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) initial.push(...startUrls);
        if (startUrl) initial.push(startUrl);
        if (url) initial.push(url);
        if (!initial.length) initial.push(buildStartUrl(keyword, location, category));

        const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);

        let saved = 0;

        async function findJobLinks(page, crawlerLog) {
            const links = new Set();

            // Wait for job listings to load
            try {
                await page.waitForSelector('a[href*="/emplois/"]', { timeout: 15000 });
                crawlerLog.info('Job listings loaded successfully');
            } catch (e) {
                crawlerLog.warning('Timeout waiting for job listings');
            }

            // Log page title
            const pageTitle = await page.title();
            crawlerLog.info(`Page title: ${pageTitle}`);

            // Check for cookie banner and dismiss
            try {
                const cookieBanner = await page.$('#didomi-notice-agree-button, [class*="cookie"] button, [class*="consent"] button');
                if (cookieBanner) {
                    await cookieBanner.click();
                    crawlerLog.info('Dismissed cookie banner');
                    await page.waitForTimeout(1000);
                }
            } catch (e) {
                // Cookie banner not found or already dismissed
            }

            // Extract all job links using page.evaluate for better performance
            const jobLinks = await page.evaluate(() => {
                const links = [];
                const anchors = document.querySelectorAll('a[href]');
                anchors.forEach(a => {
                    const href = a.href;
                    if (href && /\/emplois\/\d+\.html/i.test(href)) {
                        links.push(href);
                    }
                });
                return [...new Set(links)];
            });

            crawlerLog.info(`Found ${jobLinks.length} job links via page.evaluate`);
            
            jobLinks.forEach(link => {
                if (link.includes('hellowork.com')) {
                    links.add(link);
                }
            });

            return [...links];
        }

        function buildNextPageUrl(currentUrl) {
            const url = new URL(currentUrl);
            const currentPage = parseInt(url.searchParams.get('p') || '1');
            url.searchParams.set('p', (currentPage + 1).toString());
            return url.href;
        }

        const crawler = new PlaywrightCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            maxConcurrency: 3,
            requestHandlerTimeoutSecs: 120,
            launchContext: {
                launchOptions: {
                    headless: true,
                    args: [
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-blink-features=AutomationControlled',
                        '--lang=fr-FR'
                    ],
                },
                userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            },
            failedRequestHandler: async ({ request, error }) => {
                log.error(`Request ${request.url} failed: ${error.message}`);
            },
            async requestHandler({ request, page, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    crawlerLog.info(`Processing LIST page ${pageNo}: ${request.url}`);

                    // Wait for page to load completely
                    await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {
                        crawlerLog.warning('Network idle timeout, continuing anyway');
                    });

                    const links = await findJobLinks(page, crawlerLog);
                    crawlerLog.info(`LIST [Page ${pageNo}] -> found ${links.length} job links`);

                    if (links.length === 0) {
                        const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 800));
                        crawlerLog.warning(`No job links found. Body preview: ${bodyText}`);
                        
                        if (pageNo > 1) {
                            crawlerLog.warning(`Stopping pagination at page ${pageNo}`);
                            return;
                        }
                    }

                    if (collectDetails) {
                        const remaining = RESULTS_WANTED - saved;
                        const toEnqueue = links.slice(0, Math.max(0, remaining));
                        if (toEnqueue.length) {
                            await enqueueLinks({ 
                                urls: toEnqueue.map(url => ({ url, userData: { label: 'DETAIL' } }))
                            });
                        }
                    } else {
                        const remaining = RESULTS_WANTED - saved;
                        const toPush = links.slice(0, Math.max(0, remaining));
                        if (toPush.length) {
                            await Dataset.pushData(toPush.map(u => ({ url: u, _source: 'hellowork.com' })));
                            saved += toPush.length;
                        }
                    }

                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES && links.length > 0) {
                        const nextUrl = buildNextPageUrl(request.url);
                        await enqueueLinks({ 
                            urls: [{ url: nextUrl, userData: { label: 'LIST', pageNo: pageNo + 1 } }]
                        });
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    
                    crawlerLog.info(`Processing DETAIL page: ${request.url}`);
                    
                    try {
                        await page.waitForLoadState('domcontentloaded');
                        
                        // Extract data using Playwright's page.evaluate
                        const data = await page.evaluate(() => {
                            const result = {};
                            
                            // Title
                            const h1 = document.querySelector('h1');
                            result.title = h1 ? h1.innerText.trim() : null;
                            
                            // Company - try to extract from h1 link or company elements
                            const companyLink = document.querySelector('h1 a');
                            if (companyLink) {
                                result.company = companyLink.innerText.trim();
                            } else {
                                const companyEl = document.querySelector('[class*="company"], [class*="entreprise"]');
                                result.company = companyEl ? companyEl.innerText.trim() : null;
                            }
                            
                            // Location
                            const locationEl = document.querySelector('[class*="location"]');
                            result.location = locationEl ? locationEl.innerText.trim() : null;
                            
                            // Description
                            const descSelectors = ['.job-description', '[class*="mission"]', '[class*="profil"]', '[class*="description"]'];
                            let descriptionText = '';
                            let descriptionHtml = '';
                            for (const sel of descSelectors) {
                                const el = document.querySelector(sel);
                                if (el) {
                                    descriptionHtml += el.innerHTML;
                                    descriptionText += el.innerText + ' ';
                                }
                            }
                            result.description_html = descriptionHtml || null;
                            result.description_text = descriptionText.trim() || null;
                            
                            // Extract from body text
                            const bodyText = document.body.innerText;
                            
                            // Date
                            const dateMatch = bodyText.match(/Publi\u00e9e le (\d{2}\/\d{2}\/\d{4})/);
                            result.date_posted = dateMatch ? dateMatch[1] : null;
                            
                            // Salary
                            const salaryMatch = bodyText.match(/(\d+(?:\s?\d+)*(?:,\d+)?\s?\u20ac\s?\/\s?(?:mois|an))/);
                            result.salary = salaryMatch ? salaryMatch[1].trim() : null;
                            
                            // Contract type
                            const contractMatch = bodyText.match(/(CDI|CDD|Stage|Int\u00e9rim|Temps plein|Temps partiel)/);
                            result.contract_type = contractMatch ? contractMatch[1] : null;
                            
                            return result;
                        });

                        const item = {
                            title: data.title || null,
                            company: data.company || null,
                            category: category || null,
                            location: data.location || null,
                            salary: data.salary || null,
                            contract_type: data.contract_type || null,
                            date_posted: data.date_posted || null,
                            description_html: data.description_html || null,
                            description_text: data.description_text || null,
                            url: request.url,
                        };

                        if (item.title) {
                            await Dataset.pushData(item);
                            saved++;
                            crawlerLog.info(`Saved job: ${item.title} at ${item.company || 'Unknown company'}`);
                        } else {
                            crawlerLog.warning(`Could not extract title from ${request.url}`);
                        }
                    } catch (err) {
                        crawlerLog.error(`DETAIL ${request.url} failed: ${err.message}`);
                    }
                }
            }
        });

        log.info(`Starting scraper with ${initial.length} initial URL(s)`);
        initial.forEach((u, i) => log.info(`Initial URL ${i + 1}: ${u}`));
        
        await crawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
        
        log.info('=== SCRAPING COMPLETED ===');
        log.info(`Total jobs saved: ${saved}`);
        log.info(`Target was: ${RESULTS_WANTED}`);
        if (saved === 0) {
            log.error('WARNING: No jobs were scraped. Check logs above for errors or blocking issues.');
        }
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });
