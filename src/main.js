// Hybrid Hellowork scraper: Cheerio for LIST pages, Playwright for DETAIL pages
import { Actor, log } from 'apify';
import { PlaywrightCrawler, CheerioCrawler, Dataset } from 'crawlee';

// ---------- Shared helpers ----------

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

// ---------- MAIN ----------

await Actor.main(async () => {
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
    const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW)
        ? Math.max(1, +MAX_PAGES_RAW)
        : 999;

    const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);

    // Build initial LIST URLs
    const initialUrls = [];
    if (Array.isArray(startUrls) && startUrls.length) initialUrls.push(...startUrls);
    if (startUrl) initialUrls.push(startUrl);
    if (url) initialUrls.push(url);
    if (!initialUrls.length) initialUrls.push(buildStartUrl(keyword, location, category));

    let saved = 0;
    const detailUrls = new Set(); // for DETAIL phase

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

    const cheerioCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxRequestRetries: 2,
        maxConcurrency: 20, // Cheerio is cheap
        requestHandlerTimeoutSecs: 30,
        async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
            const label = request.userData?.label || 'LIST';
            const pageNo = request.userData?.pageNo || 1;
            if (label !== 'LIST') return;

            const links = findJobLinksCheerio($, crawlerLog);
            crawlerLog.info(
                `LIST page ${pageNo}: ${links.length} job links (saved=${saved}, target=${RESULTS_WANTED}, collectedDetails=${detailUrls.size})`,
            );

            if (links.length === 0) {
                crawlerLog.warning(`No job links found on page ${pageNo}`);
                if (pageNo > 1) {
                    crawlerLog.warning(`Stopping pagination at page ${pageNo}`);
                    return;
                }
            }

            if (collectDetails) {
                for (const link of links) {
                    if (detailUrls.size >= RESULTS_WANTED) break;
                    detailUrls.add(link);
                }
            } else {
                const remaining = RESULTS_WANTED - saved;
                const toPush = links.slice(0, Math.max(0, remaining));
                if (toPush.length) {
                    await Dataset.pushData(
                        toPush.map((u) => ({ url: u, _source: 'hellowork.com' })),
                    );
                    saved += toPush.length;
                }
            }

            if (collectDetails && detailUrls.size >= RESULTS_WANTED) {
                crawlerLog.info(
                    `Collected enough detail URLs (${detailUrls.size}), not enqueueing more pages.`,
                );
                return;
            }

            if (pageNo < MAX_PAGES && links.length > 0) {
                const nextUrl = buildNextPageUrl(request.url);
                await enqueueLinks({
                    urls: [nextUrl],
                    userData: { label: 'LIST', pageNo: pageNo + 1 },
                });
            }
        },
    });

    // ---------- PlaywrightCrawler (DETAIL pages) ----------

    const playwrightCrawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConf,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 30,
            sessionOptions: {
                maxUsageCount: 50,
                maxAgeSecs: 24 * 60 * 60,
            },
        },
        persistCookiesPerSession: true,
        // cranked up for speed (Apify autoscaler will keep it safe)
        maxConcurrency: 15,
        minConcurrency: 5,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 35,
        navigationTimeoutSecs: 20,
        // Faster navigation: no need to wait for full "load"
        navigationOptions: {
            waitUntil: 'domcontentloaded',
        },
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
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-sync',
                    '--metrics-recording-only',
                    '--no-first-run',
                    '--lang=fr-FR',
                ],
            },
        },
        browserPoolOptions: {
            useFingerprints: true, // let Apify + Crawlee fingerprint properly
            fingerprintOptions: {
                locales: ['fr-FR'],
                browsers: ['chromium'],
                timeZones: ['Europe/Paris'],
            },
            retireBrowserAfterPageCount: 60,
            maxOpenPagesPerBrowser: 2,
        },
        preNavigationHooks: [
            async ({ page }) => {
                // Block heavy resources for speed
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'stylesheet', 'font', 'media'].includes(type)) {
                        route.abort();
                    } else {
                        route.continue();
                    }
                });
            },
        ],
        failedRequestHandler: async ({ request, error }) => {
            log.error(`DETAIL failed ${request.url}: ${error.message}`);
        },
        async requestHandler({ request, page, log: crawlerLog }) {
            if (saved >= RESULTS_WANTED) return;

            try {
                // Accept cookies if needed
                try {
                    await page.click('#didomi-notice-agree-button', { timeout: 2000 });
                    await page.waitForTimeout(200);
                } catch {
                    // ignore
                }

                await page.waitForSelector('h1', { timeout: 8000 }).catch(() => {});

                // Expand truncated text if there's a toggle
                try {
                    const toggleBtn = await page.$(
                        'button[data-truncate-text-target="toggleButton"], button[data-action*="truncate-text#toggle"], button[aria-expanded]',
                    );
                    if (toggleBtn) {
                        await toggleBtn.click({ timeout: 2000 }).catch(() => {});
                        await page.waitForTimeout(150);
                    }
                } catch {
                    // ignore
                }

                const data = await page.evaluate(() => {
                    const result = {};

                    // Build a brand-new HTML tree with ONLY text tags
                    function extractTextualHtml(rootEl) {
                        if (!rootEl) return '';
                        const allowedInline = ['strong', 'b', 'em', 'i', 'br'];
                        const allowedBlock = ['p', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'h4'];

                        const doc = document.implementation.createHTMLDocument('');
                        const outRoot = doc.createElement('div');

                        function appendNode(sourceNode, targetParent) {
                            if (sourceNode.nodeType === Node.TEXT_NODE) {
                                const text = sourceNode.nodeValue;
                                if (text && text.trim()) {
                                    targetParent.appendChild(doc.createTextNode(text));
                                }
                                return;
                            }
                            if (sourceNode.nodeType !== Node.ELEMENT_NODE) return;

                            const tag = sourceNode.nodeName.toLowerCase();

                            if (
                                allowedInline.includes(tag) ||
                                allowedBlock.includes(tag)
                            ) {
                                const newEl = doc.createElement(tag);
                                targetParent.appendChild(newEl);
                                for (const child of Array.from(sourceNode.childNodes)) {
                                    appendNode(child, newEl);
                                }
                                return;
                            }

                            // Disallowed tag: inline its children into current parent
                            for (const child of Array.from(sourceNode.childNodes)) {
                                appendNode(child, targetParent);
                            }
                        }

                        appendNode(rootEl, outRoot);
                        return outRoot.innerHTML.trim();
                    }

                    // Remove obvious banners
                    const bannersToRemove = [
                        '#didomi-host',
                        '#didomi-notice',
                        '[class*="cookie"]',
                        '[class*="consent"]',
                        '[id*="cookie"]',
                        '[id*="consent"]',
                    ];
                    bannersToRemove.forEach((sel) => {
                        document.querySelectorAll(sel).forEach((el) => el.remove());
                    });

                    // Title
                    const h1 = document.querySelector('h1');
                    if (h1) {
                        const text = h1.innerText.trim();
                        const m = text.match(/^(.+?)(?:\s*\[|$)/);
                        result.title = (m ? m[1] : text).trim();
                    } else {
                        result.title = null;
                    }

                    // Company
                    const companyLink = document.querySelector('h1 a');
                    if (companyLink) {
                        result.company = companyLink.innerText.trim();
                    } else if (h1) {
                        const m = h1.innerText.match(/\[(.+?)\]/);
                        result.company = m ? m[1].trim() : null;
                    } else {
                        result.company = null;
                    }

                    // Location via JSON-LD first
                    let location = null;
                    try {
                        const scripts = Array.from(
                            document.querySelectorAll('script[type="application/ld+json"]'),
                        );
                        outer: for (const s of scripts) {
                            let json;
                            try {
                                json = JSON.parse(s.textContent || '{}');
                            } catch {
                                continue;
                            }
                            const blocks = Array.isArray(json) ? json : [json];
                            for (const block of blocks) {
                                if (block['@type'] === 'JobPosting' && block.jobLocation) {
                                    const jl = Array.isArray(block.jobLocation)
                                        ? block.jobLocation[0]
                                        : block.jobLocation;
                                    const addr = jl.address || {};
                                    const parts = [
                                        addr.addressLocality,
                                        addr.postalCode,
                                        addr.addressRegion,
                                        addr.addressCountry,
                                    ].filter(Boolean);
                                    if (parts.length) {
                                        location = parts.join(', ');
                                        break outer;
                                    }
                                }
                            }
                        }
                    } catch {
                        // ignore JSON-LD failures
                    }

                    // Fallback: DOM selectors
                    if (!location) {
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
                    }

                    // Fallback: text near title
                    if (!location && h1 && h1.parentElement) {
                        const nearTitle = h1.parentElement.querySelectorAll('p, span, div');
                        for (const el of nearTitle) {
                            const t = el.innerText.trim();
                            if (
                                /[A-Za-zéèàêçÉÈÀÖÏ ]{2,},?\s*\d{2}|Paris|Lyon|Marseille|Toulouse|Lille/i.test(
                                    t,
                                )
                            ) {
                                location = t;
                                break;
                            }
                        }
                    }
                    result.location = location;

                    // Description: pick content sections, then rebuild as text-only HTML
                    const descSelectors = [
                        '[data-cy="job-description"]',
                        'section[class*="mission"]',
                        'section[class*="profil"]',
                        'section[class*="description"]',
                        'section.tw-peer',
                        'article',
                        'main section',
                    ];

                    let descriptionText = '';
                    let descriptionHtml = '';

                    for (const sel of descSelectors) {
                        const els = document.querySelectorAll(sel);
                        els.forEach((el) => {
                            const text = el.innerText.trim();
                            if (
                                text.length > 80 &&
                                !/traceur|cookie|consentement|GDPR/i.test(text)
                            ) {
                                const sanitized = extractTextualHtml(el);
                                if (sanitized) {
                                    descriptionHtml += sanitized + '\n';
                                    descriptionText += text + '\n';
                                }
                            }
                        });
                        if (descriptionText.length > 200) break;
                    }

                    result.description_html = descriptionHtml.trim() || null;
                    result.description_text = descriptionText.trim() || null;

                    const bodyText = document.body.innerText || '';

                    const dateMatch = bodyText.match(/Publiée? le (\d{2}\/\d{2}\/\d{4})/);
                    result.date_posted = dateMatch ? dateMatch[1] : null;

                    const salaryMatch = bodyText.match(
                        /(\d+(?:\s?\d+)*(?:,\d+)?\s?€\s?\/\s?(?:mois|an))/i,
                    );
                    result.salary = salaryMatch ? salaryMatch[1].trim() : null;

                    const contractMatch = bodyText.match(
                        /(CDI|CDD|Stage|Intérim|Temps plein|Temps partiel)/i,
                    );
                    result.contract_type = contractMatch ? contractMatch[1] : null;

                    return result;
                });

                // If description is still too short, do a second attempt focusing on fewer selectors
                if (!data.description_text || data.description_text.length < 120) {
                    try {
                        await page.waitForTimeout(300);
                        const second = await page.evaluate(() => {
                            const result2 = {};

                            function extractTextualHtml(rootEl) {
                                if (!rootEl) return '';
                                const allowedInline = ['strong', 'b', 'em', 'i', 'br'];
                                const allowedBlock = [
                                    'p',
                                    'ul',
                                    'ol',
                                    'li',
                                    'h1',
                                    'h2',
                                    'h3',
                                    'h4',
                                ];
                                const doc = document.implementation.createHTMLDocument('');
                                const outRoot = doc.createElement('div');

                                function appendNode(sourceNode, targetParent) {
                                    if (sourceNode.nodeType === Node.TEXT_NODE) {
                                        const text = sourceNode.nodeValue;
                                        if (text && text.trim()) {
                                            targetParent.appendChild(
                                                doc.createTextNode(text),
                                            );
                                        }
                                        return;
                                    }
                                    if (sourceNode.nodeType !== Node.ELEMENT_NODE) return;

                                    const tag = sourceNode.nodeName.toLowerCase();
                                    if (
                                        allowedInline.includes(tag) ||
                                        allowedBlock.includes(tag)
                                    ) {
                                        const newEl = doc.createElement(tag);
                                        targetParent.appendChild(newEl);
                                        for (const child of Array.from(
                                            sourceNode.childNodes,
                                        )) {
                                            appendNode(child, newEl);
                                        }
                                        return;
                                    }
                                    for (const child of Array.from(
                                        sourceNode.childNodes,
                                    )) {
                                        appendNode(child, targetParent);
                                    }
                                }

                                appendNode(rootEl, outRoot);
                                return outRoot.innerHTML.trim();
                            }

                            const selectors = [
                                '[data-cy="job-description"]',
                                'section.tw-peer',
                                'article',
                            ];
                            for (const sel of selectors) {
                                const el = document.querySelector(sel);
                                if (el && el.innerText && el.innerText.length > 100) {
                                    const sanitized = extractTextualHtml(el);
                                    if (sanitized) {
                                        result2.description_text = el.innerText.trim();
                                        result2.description_html = sanitized;
                                        break;
                                    }
                                }
                            }
                            return result2;
                        });

                        if (
                            second &&
                            second.description_text &&
                            second.description_text.length >
                                (data.description_text || '').length
                        ) {
                            data.description_text = second.description_text;
                            data.description_html = second.description_html;
                        }
                    } catch {
                        // ignore retry errors
                    }
                }

                const item = {
                    title: cleanText(data.title) || null,
                    company: cleanText(data.company) || null,
                    location: cleanText(data.location) || null,
                    salary: cleanText(data.salary) || null,
                    contract_type: cleanText(data.contract_type) || null,
                    date_posted: cleanText(data.date_posted) || null,
                    description_html: data.description_html || null, // now text-only HTML
                    description_text: cleanText(data.description_text) || null,
                    url: request.url,
                };

                if (item.title) {
                    await Dataset.pushData(item);
                    saved++;
                    crawlerLog.info(
                        `Saved job #${saved}: ${item.title} (${item.company || 'Unknown company'})`,
                    );
                } else {
                    crawlerLog.warning(`Missing title for DETAIL page: ${request.url}`);
                }
            } catch (err) {
                crawlerLog.error(`DETAIL handler error ${request.url}: ${err.message}`);
            }
        },
    });

    // ---------- RUN HYBRID FLOW ----------

    log.info(
        `Starting HYBRID scraper with ${initialUrls.length} initial URL(s); target=${RESULTS_WANTED}, maxPages=${MAX_PAGES}`,
    );
    initialUrls.forEach((u, i) => log.info(`Initial URL ${i + 1}: ${u}`));

    log.info('Phase 1: CheerioCrawler (LIST pages, fast)');
    await cheerioCrawler.run(
        initialUrls.map((u) => ({
            url: u,
            userData: { label: 'LIST', pageNo: 1 },
        })),
    );

    const detailArray = Array.from(detailUrls);
    log.info(`LIST phase finished. Detail URLs collected: ${detailArray.length}`);

    if (collectDetails && detailArray.length > 0) {
        log.info('Phase 2: PlaywrightCrawler (DETAIL pages, JS-enabled, concurrency up to 15)');
        await playwrightCrawler.run(
            detailArray.map((u) => ({
                url: u,
            })),
        );
    } else if (collectDetails) {
        log.warning('DETAIL phase skipped: no detail URLs were collected.');
    }

    log.info('=== HYBRID SCRAPING COMPLETED ===');
    log.info(`Total jobs saved: ${saved}`);
    log.info(`Target was: ${RESULTS_WANTED}`);
    if (saved === 0) {
        log.error(
            'WARNING: No jobs were scraped. Check selectors, blocking, or recent DOM changes on Hellowork.',
        );
    }
});
