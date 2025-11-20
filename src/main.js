// Hellowork jobs scraper - Hybrid implementation (Cheerio for lists, Playwright for details)
import { Actor, log } from 'apify';
import { PlaywrightCrawler, CheerioCrawler, Dataset } from 'crawlee';
import * as cheerio from 'cheerio';

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

        // Fast Cheerio-based extraction for LIST pages (server-rendered HTML)
        function findJobLinksCheerio($, crawlerLog) {
            const links = new Set();
            
            $('a[href*="/emplois/"]').each((i, el) => {
                const href = $(el).attr('href');
                if (href && /\/emplois\/\d+\.html/i.test(href)) {
                    const absoluteUrl = toAbs(href);
                    if (absoluteUrl && absoluteUrl.includes('hellowork.com')) {
                        links.add(absoluteUrl);
                    }
                }
            });
            
            crawlerLog.info(`Found ${links.size} job links via Cheerio (fast)`);
            return [...links];
        }

        function buildNextPageUrl(currentUrl) {
            const url = new URL(currentUrl);
            const currentPage = parseInt(url.searchParams.get('p') || '1');
            url.searchParams.set('p', (currentPage + 1).toString());
            return url.href;
        }

        // CheerioCrawler for fast LIST page scraping (server-rendered HTML)
        const cheerioCrawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 2,
            maxConcurrency: 20,
            requestHandlerTimeoutSecs: 30,
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    crawlerLog.info(`Processing LIST page ${pageNo} with Cheerio (fast): ${request.url}`);

                    const links = findJobLinksCheerio($, crawlerLog);
                    crawlerLog.info(`LIST [Page ${pageNo}] -> found ${links.length} job links`);

                    if (links.length === 0) {
                        crawlerLog.warning(`No job links found on page ${pageNo}`);
                        if (pageNo > 1) {
                            crawlerLog.warning(`Stopping pagination at page ${pageNo}`);
                            return;
                        }
                    }

                    if (collectDetails) {
                        const remaining = RESULTS_WANTED - saved;
                        const toEnqueue = links.slice(0, Math.max(0, remaining));
                        if (toEnqueue.length) {
                            // Enqueue detail pages for Playwright crawler
                            for (const url of toEnqueue) {
                                await playwrightCrawler.addRequests([{ 
                                    url,
                                    userData: { label: 'DETAIL' }
                                }]);
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
            }
        });

        // PlaywrightCrawler ONLY for DETAIL pages requiring JavaScript
        const playwrightCrawler = new PlaywrightCrawler({
            proxyConfiguration: proxyConf,
            useSessionPool: true,
            // For stealth, use proxy sessions and enable rotation and sticky sessions
            sessionPoolOptions: {
                persistStateKey: 'session',
                maxPoolSize: 50,
            },
            persistCookiesPerSession: true,
            sessionPoolOptions: {
                // Keep sessions small and rotate frequently to avoid detection
                maxPoolSize: 50,
                sessionOptions: {
                    maxUsageCount: 50,
                    maxAgeSecs: 24 * 60 * 60,
                },
            },
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
                // Use rotating fingerprints for stealth; keep pages-per-browser low for memory
                useFingerprints: true,
                fingerprintOptions: {
                    locales: ['fr-FR'],
                    browsers: ['chromium'],
                    timeZones: ['Europe/Paris']
                },
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
                // Small randomized delays to mimic human behaviour (micro-jitter)
                await page.waitForTimeout(Math.random() * 300 + 50);
                // Try to set a slightly randomized viewport or UA per page (additional stealth)
                try {
                    await page.setViewportSize({ width: 1200 + Math.floor(Math.random() * 100), height: 800 + Math.floor(Math.random() * 50) });
                } catch (e) {}

                // Rotate user agent per page for stealth
                try {
                    const uas = [
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0'
                    ];
                    const ua = uas[Math.floor(Math.random() * uas.length)];
                    await page.setUserAgent(ua);
                } catch (e) {}

                // Small human-like mouse move
                try {
                    const x = 100 + Math.floor(Math.random() * 400);
                    const y = 100 + Math.floor(Math.random() * 200);
                    await page.mouse.move(x, y, { steps: 5 });
                } catch (e) {}
            }],
            failedRequestHandler: async ({ request, error }) => {
                log.error(`Request ${request.url} failed: ${error.message}`);
            },
            async requestHandler({ request, page, log: crawlerLog }) {
                const label = request.userData?.label || 'DETAIL';

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
                        
                        // Wait for job content to load and allow small expand clicks
                        await page.waitForSelector('h1', { timeout: 8000 }).catch(() => {});
                        // Click 'Show more' / expand if present to get full description
                        try {
                            const toggleBtn = await page.$('button[data-truncate-text-target="toggleButton"], button[data-action*="truncate-text#toggle"], button[aria-expanded]');
                            if (toggleBtn) {
                                await toggleBtn.click({ timeout: 3000 }).catch(() => {});
                                await page.waitForTimeout(200);
                            }
                        } catch (e) {}
                        // Additional small random delay before extracting
                        await page.waitForTimeout(Math.random() * 300 + 50);
                        
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
                            
                            // Location - look in metadata or body and use heuristics
                            let location = null;
                            const metaSelectors = [
                                '[data-cy="job-location"]',
                                '[class*="location"]',
                                '[itemprop="jobLocation"]',
                                'a[href*="/locations/"]',
                                '.tw-inline-block[role*="location"]',
                            ];
                            for (const sel of metaSelectors) {
                                const el = document.querySelector(sel);
                                if (el && el.innerText.trim()) {
                                    location = el.innerText.trim();
                                    break;
                                }
                            }
                            // Fallback: find text near the title that contains typical place patterns (comma or department code)
                            if (!location) {
                                const nearTitle = h1 ? h1.parentElement.querySelectorAll('p, span, div') : [];
                                for (const el of nearTitle) {
                                    const t = el.innerText.trim();
                                    if (/[A-Za-zéèàêçÉÈÀÖÏ ]{2,},?\s*\d{2}|Paris|Lyon|Marseille|Toulouse|Lille/.test(t)) {
                                        location = t;
                                        break;
                                    }
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
                                        // Strip classes and inline attributes for clean HTML
                                        const clone = el.cloneNode(true);
                                        const tmp = document.createElement('div');
                                        tmp.appendChild(clone);
                                        // Allowed tags
                                        const allowed = ['P','BR','UL','OL','LI','STRONG','B','EM','I','H1','H2','H3','H4'];
                                        function sanitizeNode(node) {
                                            // Remove attributes
                                            if (node.nodeType === Node.ELEMENT_NODE) {
                                                // Replace with itself if allowed otherwise unwrap
                                                if (!allowed.includes(node.nodeName)) {
                                                    const parent = node.parentNode;
                                                    if (!parent) return;
                                                    while (node.firstChild) parent.insertBefore(node.firstChild, node);
                                                    parent.removeChild(node);
                                                    return;
                                                }
                                                // Remove all attributes
                                                Array.from(node.attributes).forEach(a => node.removeAttribute(a.name));
                                            }
                                            // Recurse
                                            let child = node.firstChild;
                                            while (child) {
                                                sanitizeNode(child);
                                                child = child.nextSibling;
                                            }
                                        }
                                        sanitizeNode(tmp);
                                        descriptionHtml += tmp.innerHTML + '\n';
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

                        // Retry when the description is too short or empty
                        if ((!data.description_text || data.description_text.length < 150) && data.title) {
                            try {
                                // Second chance: wait for rendering and click any remaining expanders
                                await page.waitForTimeout(800);
                                const showBtn = await page.$('button[data-truncate-text-target="toggleButton"], button[aria-expanded]');
                                if (showBtn) await showBtn.click().catch(() => {});
                                await page.waitForTimeout(500);
                                const secondData = await page.evaluate(() => {
                                    // Similar extraction but prefer visible sections
                                    const result2 = {};
                                    result2.description_text = null;
                                    const descSelectors2 = ['[data-cy="job-description"]', 'section.tw-section', 'article', 'main section'];
                                    for (const sel of descSelectors2) {
                                        const el = document.querySelector(sel);
                                        if (el && el.innerText && el.innerText.length > 140) {
                                            result2.description_text = el.innerText.trim();
                                            result2.description_html = el.innerHTML;
                                            break;
                                        }
                                    }
                                    return result2;
                                });
                                if (secondData && secondData.description_text && secondData.description_text.length > (data.description_text || '').length) {
                                    data.description_text = secondData.description_text;
                                    data.description_html = secondData.description_html;
                                }
                            } catch (e) {
                                // continue without retry
                            }
                        }

                        const item = {
                            title: cleanText(data.title) || null,
                            company: cleanText(data.company) || null,
                            location: cleanText(data.location) || null,
                            salary: cleanText(data.salary) || null,
                            contract_type: cleanText(data.contract_type) || null,
                            date_posted: cleanText(data.date_posted) || null,
                            description_html: data.description_html || null,
                            description_text: cleanText(data.description_text) || null,
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

        log.info(`Starting HYBRID scraper with ${initial.length} initial URL(s)`);
        log.info('Phase 1: CheerioCrawler (fast) for LIST pages');
        log.info('Phase 2: PlaywrightCrawler (JS-enabled) for DETAIL pages only');
        initial.forEach((u, i) => log.info(`Initial URL ${i + 1}: ${u}`));
        
        // Run CheerioCrawler first for LIST pages (fast)
        await cheerioCrawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
        
        // Then run PlaywrightCrawler for DETAIL pages (slower but needed for JS)
        if (collectDetails) {
            log.info('Starting DETAIL page extraction with Playwright...');
            await playwrightCrawler.run();
        }
        
        log.info('=== HYBRID SCRAPING COMPLETED ===');
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
