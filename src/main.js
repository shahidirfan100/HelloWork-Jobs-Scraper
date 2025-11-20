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

            // Quick wait for job listings
            try {
                await page.waitForSelector('a[href*="/emplois/"]', { timeout: 5000 });
            } catch (e) {
                crawlerLog.warning('Job listings may not be loaded');
            }

            // Log page title
            const pageTitle = await page.title();
            crawlerLog.info(`Page title: ${pageTitle}`);

            // Quick cookie banner dismissal
            try {
                const banner = await page.$('#didomi-notice-agree-button');
                if (banner) {
                    await banner.click();
                    crawlerLog.info('Dismissed cookie banner');
                    await page.waitForTimeout(300);
                }
            } catch (e) {
                // Banner not present
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
            maxRequestRetries: 2,
            maxConcurrency: 10,
            minConcurrency: 5,
            requestHandlerTimeoutSecs: 45,
            navigationTimeoutSecs: 30,
            launchContext: {
                launchOptions: {
                    headless: true,
                    args: [
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-images',
                        '--disable-css',
                        '--disable-fonts',
                        '--disable-extensions',
                        '--disable-plugins',
                        '--mute-audio',
                        '--disable-background-networking',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-breakpad',
                        '--disable-component-extensions-with-background-pages',
                        '--disable-default-apps',
                        '--disable-features=TranslateUI',
                        '--disable-hang-monitor',
                        '--disable-ipc-flooding-protection',
                        '--disable-popup-blocking',
                        '--disable-prompt-on-repost',
                        '--disable-renderer-backgrounding',
                        '--disable-sync',
                        '--force-color-profile=srgb',
                        '--metrics-recording-only',
                        '--no-first-run',
                        '--enable-automation',
                        '--password-store=basic',
                        '--use-mock-keychain',
                        '--lang=fr-FR'
                    ],
                },
                userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            },
            browserPoolOptions: {
                useFingerprints: false,
                retireBrowserAfterPageCount: 50,
                maxOpenPagesPerBrowser: 1,
            },
            preNavigationHooks: [async ({ page, request }) => {
                // Block unnecessary resources for maximum speed
                await page.route('**/*', (route) => {
                    const resourceType = route.request().resourceType();
                    if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
                        route.abort();
                    } else {
                        route.continue();
                    }
                });
            }],
            failedRequestHandler: async ({ request, error }) => {
                log.error(`Request ${request.url} failed: ${error.message}`);
            },
            async requestHandler({ request, page, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    crawlerLog.info(`Processing LIST page ${pageNo}: ${request.url}`);

                    // Wait minimal time for content
                    await page.waitForLoadState('domcontentloaded', { timeout: 10000 }).catch(() => {});

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
                            for (const url of toEnqueue) {
                                await enqueueLinks({ 
                                    urls: [url],
                                    userData: { label: 'DETAIL' }
                                });
                            }
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
                            urls: [nextUrl],
                            userData: { label: 'LIST', pageNo: pageNo + 1 }
                        });
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    
                    crawlerLog.info(`Processing DETAIL page: ${request.url}`);
                    
                    try {
                        // Dismiss cookie banner first
                        try {
                            await page.click('#didomi-notice-agree-button', { timeout: 2000 });
                            await page.waitForTimeout(500);
                        } catch (e) {
                            // Banner already dismissed or not present
                        }
                        
                        // Wait for job content to load
                        await page.waitForSelector('h1', { timeout: 8000 }).catch(() => {});
                        
                        // Extract data using Playwright's page.evaluate
                        const data = await page.evaluate(() => {
                            const result = {};
                            
                            // Remove cookie banner and consent modals from DOM
                            const bannersToRemove = [
                                '#didomi-host',
                                '#didomi-notice',
                                '[class*="cookie"]',
                                '[class*="consent"]',
                                '[id*="cookie"]',
                                '[id*="consent"]'
                            ];
                            bannersToRemove.forEach(sel => {
                                document.querySelectorAll(sel).forEach(el => el.remove());
                            });
                            
                            // Title - extract from h1 but remove company name if present
                            const h1 = document.querySelector('h1');
                            if (h1) {
                                const h1Text = h1.innerText.trim();
                                // Title format is often "Job Title [COMPANY]"
                                const titleMatch = h1Text.match(/^(.+?)(?:\s*\[|$)/);
                                result.title = titleMatch ? titleMatch[1].trim() : h1Text;
                            } else {
                                result.title = null;
                            }
                            
                            // Company - extract from h1 link or brackets
                            const companyLink = document.querySelector('h1 a');
                            if (companyLink) {
                                result.company = companyLink.innerText.trim();
                            } else if (h1) {
                                const h1Text = h1.innerText;
                                const companyMatch = h1Text.match(/\[(.+?)\]/);
                                result.company = companyMatch ? companyMatch[1].trim() : null;
                            } else {
                                result.company = null;
                            }
                            
                            // Location - look in metadata or body
                            let location = null;
                            const metaSelectors = [
                                '[data-cy="job-location"]',
                                '[class*="location"]',
                                '[itemprop="jobLocation"]'
                            ];
                            for (const sel of metaSelectors) {
                                const el = document.querySelector(sel);
                                if (el && el.innerText.trim()) {
                                    location = el.innerText.trim();
                                    break;
                                }
                            }
                            result.location = location;
                            
                            // Description - look for actual job description sections
                            const descSelectors = [
                                'section.tw-section',
                                '[data-cy="job-description"]',
                                '.job-description',
                                'section[class*="description"]',
                                'div[class*="mission"]',
                                'div[class*="profil"]',
                                'article',
                                'main section'
                            ];
                            
                            let descriptionText = '';
                            let descriptionHtml = '';
                            
                            for (const sel of descSelectors) {
                                const elements = document.querySelectorAll(sel);
                                elements.forEach(el => {
                                    const text = el.innerText.trim();
                                    // Filter out cookie/consent text and short snippets
                                    if (text.length > 100 && 
                                        !text.includes('traceur') && 
                                        !text.includes('cookie') &&
                                        !text.includes('consentement') &&
                                        !text.includes('GDPR')) {
                                        descriptionHtml += el.innerHTML + '\n';
                                        descriptionText += text + '\n';
                                    }
                                });
                                if (descriptionText.length > 200) break;
                            }
                            
                            result.description_html = descriptionHtml.trim() || null;
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
