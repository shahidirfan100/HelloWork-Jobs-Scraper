// Fast, stealthy HelloWork scraper: HTTP-first with JSON-LD extraction
import { Actor, log } from 'apify';
import { CheerioCrawler, PlaywrightCrawler, Dataset } from 'crawlee';
import { gotScraping } from 'got-scraping';

// ---------- USER AGENT ROTATION ----------

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
];

const getRandomUA = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

// ---------- DELAY HELPERS ----------

const randomDelay = (min = 300, max = 1200) =>
    new Promise((resolve) => setTimeout(resolve, min + Math.random() * (max - min)));

// ---------- SHARED HELPERS ----------

const toAbs = (href, base = 'https://www.hellowork.com') => {
    try {
        return new URL(href, base).href;
    } catch {
        return null;
    }
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

// ---------- JSON-LD EXTRACTION (PRIMARY METHOD) ----------

function extractJobFromJsonLd($, url) {
    const scripts = $('script[type="application/ld+json"]');

    for (let i = 0; i < scripts.length; i++) {
        try {
            const content = $(scripts[i]).html();
            if (!content) continue;

            const json = JSON.parse(content);
            const blocks = Array.isArray(json) ? json : [json];

            for (const block of blocks) {
                if (block['@type'] === 'JobPosting') {
                    // Extract location from jobLocation
                    let location = null;
                    if (block.jobLocation) {
                        const jl = Array.isArray(block.jobLocation) ? block.jobLocation[0] : block.jobLocation;
                        const addr = jl?.address || {};
                        const parts = [
                            addr.addressLocality,
                            addr.postalCode,
                            addr.addressRegion,
                            addr.addressCountry,
                        ].filter(Boolean);
                        if (parts.length) location = parts.join(', ');
                    }

                    // Extract salary
                    let salary = null;
                    if (block.baseSalary) {
                        const bs = block.baseSalary;
                        if (bs.value) {
                            const val = bs.value;
                            if (typeof val === 'object' && val.minValue && val.maxValue) {
                                salary = `${val.minValue} - ${val.maxValue} ${bs.currency || '€'}`;
                            } else if (typeof val === 'number' || typeof val === 'string') {
                                salary = `${val} ${bs.currency || '€'}`;
                            }
                        }
                    }

                    // Extract company name
                    let company = null;
                    if (block.hiringOrganization) {
                        company = block.hiringOrganization.name || block.hiringOrganization;
                    }

                    // Extract description (clean HTML)
                    let descriptionHtml = block.description || null;
                    let descriptionText = null;
                    if (descriptionHtml) {
                        // Remove HTML tags for plain text version
                        descriptionText = descriptionHtml.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
                    }

                    // Extract contract type
                    let contractType = null;
                    if (block.employmentType) {
                        const et = Array.isArray(block.employmentType) ? block.employmentType[0] : block.employmentType;
                        const typeMap = {
                            'FULL_TIME': 'CDI',
                            'PART_TIME': 'Temps partiel',
                            'CONTRACTOR': 'Freelance',
                            'TEMPORARY': 'CDD',
                            'INTERN': 'Stage',
                            'APPRENTICESHIP': 'Alternance',
                        };
                        contractType = typeMap[et] || et;
                    }

                    return {
                        title: cleanText(block.title) || null,
                        company: cleanText(company) || null,
                        location: cleanText(location) || null,
                        salary: cleanText(salary) || null,
                        contract_type: cleanText(contractType) || null,
                        date_posted: block.datePosted || null,
                        description_html: descriptionHtml,
                        description_text: cleanText(descriptionText) || null,
                        url,
                        _source: 'json-ld',
                    };
                }
            }
        } catch (err) {
            // Continue to next script tag
        }
    }
    return null;
}

// ---------- HTML FALLBACK EXTRACTION ----------

function extractJobFromHtml($, url) {
    const result = {
        title: null,
        company: null,
        location: null,
        salary: null,
        contract_type: null,
        date_posted: null,
        description_html: null,
        description_text: null,
        url,
        _source: 'html',
    };

    // Title
    const h1 = $('h1').first();
    if (h1.length) {
        const text = cleanText(h1.text());
        const m = text.match(/^(.+?)(?:\s*\[|$)/);
        result.title = m ? m[1].trim() : text;
    }

    // Company from h1 link or bracket
    const companyLink = $('h1 a').first();
    if (companyLink.length) {
        result.company = cleanText(companyLink.text());
    } else if (h1.length) {
        const m = h1.text().match(/\[(.+?)\]/);
        if (m) result.company = m[1].trim();
    }

    // Location from selectors
    const locationSelectors = [
        '[data-cy="job-location"]',
        '[class*="location"]',
        '[itemprop="jobLocation"]',
        'a[href*="/locations/"]',
    ];
    for (const sel of locationSelectors) {
        const el = $(sel).first();
        if (el.length && cleanText(el.text())) {
            result.location = cleanText(el.text());
            break;
        }
    }

    // Description
    const descEl = $('div[data-truncate-text-target="content"]').first();
    if (descEl.length) {
        result.description_html = descEl.html();
        result.description_text = cleanText(descEl.text());
    } else {
        // Fallback to article
        const article = $('article').first();
        if (article.length) {
            result.description_text = cleanText(article.text()).slice(0, 2000);
        }
    }

    // Date posted
    const bodyText = $('body').text() || '';
    const dateMatch = bodyText.match(/Publiée? le (\d{2}\/\d{2}\/\d{4})/);
    if (dateMatch) result.date_posted = dateMatch[1];

    // Salary
    const salaryMatch = bodyText.match(/(\d+(?:\s?\d+)*(?:,\d+)?\s?€\s?\/\s?(?:mois|an))/i);
    if (salaryMatch) result.salary = salaryMatch[1].trim();

    // Contract type
    const contractMatch = bodyText.match(/(CDI|CDD|Stage|Intérim|Temps plein|Temps partiel|Alternance)/i);
    if (contractMatch) result.contract_type = contractMatch[1];

    return result;
}

// ---------- MAIN ----------

Actor.main(async () => {
    const startTime = Date.now();
    const HARD_TIME_LIMIT_MS = 260_000;

    const input = (await Actor.getInput()) || {};

    const {
        keyword = '',
        location = '',
        category = '',
        results_wanted: RESULTS_WANTED_RAW = 100,
        max_pages: MAX_PAGES_RAW = 999,
        collectDetails = true,
        startUrl,
        startUrls,
        url,
        proxyConfiguration,
    } = input;

    const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW)
        ? Math.max(1, +RESULTS_WANTED_RAW)
        : Number.MAX_SAFE_INTEGER;
    const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 999;

    const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);

    // Initial LIST URLs
    const initialUrls = [];
    if (Array.isArray(startUrls) && startUrls.length) initialUrls.push(...startUrls);
    if (startUrl) initialUrls.push(startUrl);
    if (url) initialUrls.push(url);
    if (!initialUrls.length) initialUrls.push(buildStartUrl(keyword, location, category));

    let saved = 0;
    const detailUrls = new Set();
    const seenUrls = new Set(); // Deduplication

    // ---------- STEALTH HEADERS ----------

    const getHeaders = () => ({
        'User-Agent': getRandomUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    });

    // ---------- LIST helpers (Cheerio) ----------

    function findJobLinksCheerio($, crawlerLog) {
        const links = new Set();
        const jobLinkRegex = /\/emplois\/\d+(?:-[a-z0-9-]+)?(?:\.html)?/i;

        $('a[href*="/emplois/"]').each((_, el) => {
            const href = $(el).attr('href');
            if (!href) return;
            if (!jobLinkRegex.test(href)) return;
            const absoluteUrl = toAbs(href);
            if (absoluteUrl && absoluteUrl.includes('hellowork.com')) {
                links.add(absoluteUrl);
            }
        });

        crawlerLog.info(`Cheerio: found ${links.size} job links on this page`);
        return [...links];
    }

    function buildNextPageUrl(currentUrl) {
        const u = new URL(currentUrl);
        const currentPage = parseInt(u.searchParams.get('p') || '1', 10);
        u.searchParams.set('p', String(currentPage + 1));
        return u.href;
    }

    // ---------- CheerioCrawler (LIST pages) ----------

    const listCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxRequestRetries: 3,
        maxConcurrency: 15,
        requestHandlerTimeoutSecs: 30,
        navigationTimeoutSecs: 20,
        preNavigationHooks: [
            async ({ request }) => {
                request.headers = getHeaders();
            },
        ],
        async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
            const pageNo = request.userData?.pageNo || 1;

            // Random delay for stealth
            await randomDelay(200, 600);

            const links = findJobLinksCheerio($, crawlerLog);
            crawlerLog.info(
                `LIST page ${pageNo}: ${links.length} job links (collected=${detailUrls.size}, target=${RESULTS_WANTED})`,
            );

            if (links.length === 0 && pageNo > 1) {
                crawlerLog.warning(`No job links found on page ${pageNo}, stopping pagination`);
                return;
            }

            // Collect detail URLs
            for (const link of links) {
                if (detailUrls.size >= RESULTS_WANTED) break;
                if (!seenUrls.has(link)) {
                    seenUrls.add(link);
                    detailUrls.add(link);
                }
            }

            if (detailUrls.size >= RESULTS_WANTED) {
                crawlerLog.info(`Collected enough URLs (${detailUrls.size}), stopping pagination`);
                return;
            }

            // Enqueue next page
            if (pageNo < MAX_PAGES && links.length > 0) {
                const nextUrl = buildNextPageUrl(request.url);
                await enqueueLinks({
                    urls: [nextUrl],
                    userData: { pageNo: pageNo + 1 },
                });
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.warning(`LIST page failed ${request.url}: ${error.message}`);
        },
    });

    // ---------- CheerioCrawler (DETAIL pages - HTTP-first with JSON-LD) ----------

    const detailCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxRequestRetries: 3,
        maxConcurrency: 20,
        requestHandlerTimeoutSecs: 25,
        navigationTimeoutSecs: 15,
        preNavigationHooks: [
            async ({ request }) => {
                request.headers = getHeaders();
            },
        ],
        async requestHandler({ request, $, log: crawlerLog }) {
            if (saved >= RESULTS_WANTED) return;
            if (Date.now() - startTime > HARD_TIME_LIMIT_MS) {
                crawlerLog.info(`Time budget reached, stopping at job #${saved}`);
                return;
            }

            // Random delay for stealth
            await randomDelay(150, 500);

            const url = request.url;

            // Priority 1: Try JSON-LD extraction
            let jobData = extractJobFromJsonLd($, url);

            // Priority 2: Fallback to HTML parsing
            if (!jobData || !jobData.title) {
                crawlerLog.debug(`JSON-LD extraction failed for ${url}, trying HTML parsing`);
                jobData = extractJobFromHtml($, url);
            }

            // Validate and save
            if (jobData && jobData.title) {
                await Dataset.pushData(jobData);
                saved++;
                crawlerLog.info(
                    `✓ Saved #${saved}: ${jobData.title} (${jobData.company || 'Unknown'}) [${jobData._source}]`,
                );
            } else {
                crawlerLog.warning(`Missing title for ${url}, adding to retry queue`);
                // Add to Playwright fallback queue
                request.userData.needsPlaywright = true;
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.warning(`DETAIL page failed ${request.url}: ${error.message}`);
            request.userData.needsPlaywright = true;
        },
    });

    // ---------- PlaywrightCrawler (FALLBACK for blocked pages) ----------

    const playwrightFallbackUrls = [];

    const playwrightCrawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConf,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 10,
            sessionOptions: { maxUsageCount: 30, maxAgeSecs: 60 * 60 },
        },
        persistCookiesPerSession: true,
        maxConcurrency: 5,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 45,
        navigationTimeoutSecs: 20,
        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--mute-audio',
                    '--disable-background-networking',
                    '--lang=fr-FR',
                ],
            },
        },
        browserPoolOptions: {
            useFingerprints: true,
            fingerprintOptions: {
                locales: ['fr-FR'],
                browsers: ['chromium'],
                timeZones: ['Europe/Paris'],
            },
            retireBrowserAfterPageCount: 30,
            maxOpenPagesPerBrowser: 2,
        },
        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
                // Block heavy resources
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'stylesheet', 'font', 'media'].includes(type)) {
                        route.abort();
                    } else {
                        route.continue();
                    }
                });
                gotoOptions.waitUntil = 'domcontentloaded';
            },
        ],
        async requestHandler({ request, page, log: crawlerLog }) {
            if (saved >= RESULTS_WANTED) return;

            try {
                // Dismiss cookie banner
                try {
                    await page.click('#didomi-notice-agree-button', { timeout: 2000 });
                    await page.waitForTimeout(200);
                } catch {
                    // ignore
                }

                // Dismiss country modal by selecting France
                try {
                    const selectExists = await page.$('select');
                    if (selectExists) {
                        await page.selectOption('select', { label: 'France' });
                        await page.click('button:has-text("OK")', { timeout: 2000 });
                        await page.waitForTimeout(300);
                    }
                } catch {
                    // ignore
                }

                await page.waitForSelector('h1', { timeout: 8000 }).catch(() => { });

                const html = await page.content();
                const cheerio = await import('cheerio');
                const $ = cheerio.load(html);

                // Try JSON-LD first
                let jobData = extractJobFromJsonLd($, request.url);
                if (!jobData || !jobData.title) {
                    jobData = extractJobFromHtml($, request.url);
                    if (jobData) jobData._source = 'playwright-html';
                } else {
                    jobData._source = 'playwright-jsonld';
                }

                if (jobData && jobData.title) {
                    await Dataset.pushData(jobData);
                    saved++;
                    crawlerLog.info(
                        `✓ [Playwright] Saved #${saved}: ${jobData.title} (${jobData.company || 'Unknown'})`,
                    );
                } else {
                    crawlerLog.warning(`Playwright also failed for ${request.url}`);
                }
            } catch (err) {
                crawlerLog.error(`Playwright handler error ${request.url}: ${err.message}`);
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.error(`Playwright failed ${request.url}: ${error.message}`);
        },
    });

    // ---------- RUN FLOW ----------

    log.info('=== HelloWork Scraper (HTTP-First + JSON-LD) ===');
    log.info(`Target: ${RESULTS_WANTED} jobs, Max pages: ${MAX_PAGES}`);
    log.info(`Initial URLs: ${initialUrls.length}`);

    // Phase 1: Collect job URLs from LIST pages
    log.info('\n📋 Phase 1: Collecting job URLs from search results...');
    await listCrawler.run(
        initialUrls.map((u) => ({
            url: u,
            userData: { pageNo: 1 },
        })),
    );

    const detailArray = Array.from(detailUrls);
    log.info(`✓ Found ${detailArray.length} job URLs`);

    // Phase 2: Extract job data via HTTP (fast)
    if (collectDetails && detailArray.length > 0) {
        log.info('\n⚡ Phase 2: Extracting job data via HTTP (fast)...');
        await detailCrawler.run(detailArray.map((u) => ({ url: u })));

        // Check if any pages need Playwright fallback
        const failedUrls = detailArray.filter((u) => !seenUrls.has(`processed:${u}`));
        if (playwrightFallbackUrls.length > 0 && saved < RESULTS_WANTED) {
            log.info(`\n🎭 Phase 3: Playwright fallback for ${playwrightFallbackUrls.length} blocked pages...`);
            await playwrightCrawler.run(playwrightFallbackUrls.map((u) => ({ url: u })));
        }
    } else if (!collectDetails) {
        // Just save URLs without details
        for (const u of detailArray.slice(0, RESULTS_WANTED - saved)) {
            await Dataset.pushData({ url: u, _source: 'list-only' });
            saved++;
        }
    }

    // Summary
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    log.info('\n=== SCRAPING COMPLETED ===');
    log.info(`✓ Total jobs saved: ${saved}`);
    log.info(`✓ Target was: ${RESULTS_WANTED}`);
    log.info(`✓ Elapsed: ${elapsed}s`);
    log.info(`✓ Speed: ${(saved / parseFloat(elapsed) || 0).toFixed(2)} jobs/sec`);

    if (saved === 0) {
        log.warning(
            'No jobs were scraped. Possible causes: blocking, site changes, or no results for query.',
        );
    }
});
