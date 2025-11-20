// Hellowork jobs scraper - Hybrid implementation (Cheerio for lists, Playwright for details)
import { Actor, log } from 'apify';
import { PlaywrightCrawler, CheerioCrawler, Dataset } from 'crawlee';

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

Actor.main(async () => {
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

    // Initial URLs
    const initialUrls = [];
    if (Array.isArray(startUrls) && startUrls.length) initialUrls.push(...startUrls);
    if (startUrl) initialUrls.push(startUrl);
    if (url) initialUrls.push(url);
    if (!initialUrls.length) initialUrls.push(buildStartUrl(keyword, location, category));

    let saved = 0;
    const detailUrls = new Set();

    // ---------------------------
    // Helpers for LIST pages
    // ---------------------------

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
        const urlObj = new URL(currentUrl);
        const currentPage = parseInt(urlObj.searchParams.get('p') || '1', 10);
        urlObj.searchParams.set('p', String(currentPage + 1));
        return urlObj.href;
    }

    // ---------------------------
    // CheerioCrawler (LIST pages)
    // ---------------------------

    const cheerioCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxRequestRetries: 2,
        maxConcurrency: 15,
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

            // Stop paginating when we have enough detail URLs
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

    // ---------------------------
    // PlaywrightCrawler (DETAIL pages)
    // ---------------------------

    const playwrightCrawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConf,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 40,
            sessionOptions: {
                maxUsageCount: 40,
                maxAgeSecs: 24 * 60 * 60,
            },
        },
        persistCookiesPerSession: true,
        maxRequestRetries: 2,
        maxConcurrency: 6,
        minConcurrency: 2,
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
                    '--disable-extensions',
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
            useFingerprints: true,
            fingerprintOptions: {
                locales: ['fr-FR'],
                browsers: ['chromium'],
                timeZones: ['Europe/Paris'],
            },
            retireBrowserAfterPageCount: 40,
            maxOpenPagesPerBrowser: 1,
        },
        preNavigationHooks: [
            async ({ page }) => {
                // Block heavy resources
                await page.route('**/*', (route) => {
                    const resourceType = route.request().resourceType();
                    if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
                        route.abort();
                    } else {
                        route.continue();
                    }
                });

                await page.waitForTimeout(50 + Math.random() * 300);

                try {
                    await page.setViewportSize({
                        width: 1200 + Math.floor(Math.random() * 100),
                        height: 800 + Math.floor(Math.random() * 50),
                    });
                } catch {
                    // ignore
                }
            },
        ],
        failedRequestHandler: async ({ request, error }) => {
            log.error(`DETAIL failed ${request.url}: ${error.message}`);
        },
        async requestHandler({ request, page, log: crawlerLog }) {
            if (saved >= RESULTS_WANTED) return;

            try {
                // Cookie banner
                try {
                    await page.click('#didomi-notice-agree-button', { timeout: 2000 });
                    await page.waitForTimeout(500);
                } catch {
                    // no banner
                }

                await page.waitForSelector('h1', { timeout: 8000 }).catch(() => {});

                // Expand truncated content
                try {
                    const toggleBtn = await page.$(
                        'button[data-truncate-text-target="toggleButton"], button[data-action*="truncate-text#toggle"], button[aria-expanded]',
                    );
                    if (toggleBtn) {
                        await toggleBtn.click({ timeout: 3000 }).catch(() => {});
                        await page.waitForTimeout(200);
                    }
                } catch {
                    // ignore
                }

                await page.waitForTimeout(50 + Math.random() * 300);

                const data = await page.evaluate(() => {
                    const result = {};

                    function sanitizeToTextHtml(rootEl) {
                        if (!rootEl) return '';
                        const allowed = new Set([
                            'P',
                            'BR',
                            'UL',
                            'OL',
                            'LI',
                            'STRONG',
                            'B',
                            'EM',
                            'I',
                            'H1',
                            'H2',
                            'H3',
                            'H4',
                        ]);
                        const wrapper = document.createElement('div');
                        wrapper.appendChild(rootEl.cloneNode(true));

                        function walk(node) {
                            if (node.nodeType === Node.ELEMENT_NODE) {
                                const tag = node.nodeName;
                                if (!allowed.has(tag)) {
                                    const parent = node.parentNode;
                                    if (!parent) return;
                                    while (node.firstChild) {
                                        parent.insertBefore(node.firstChild, node);
                                    }
                                    parent.removeChild(node);
                                    return;
                                }
                                for (const attr of Array.from(node.attributes)) {
                                    node.removeAttribute(attr.name);
                                }
                            }
                            let child = node.firstChild;
                            while (child) {
                                const next = child.nextSibling;
                                walk(child);
                                child = next;
                            }
                        }

                        walk(wrapper);
                        return wrapper.innerHTML.trim();
                    }

                    // Remove cookie/consent banners
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

                    // Fallback: text around title
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

                    // Description
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
                        const elements = document.querySelectorAll(sel);
                        elements.forEach((el) => {
                            const text = el.innerText.trim();
                            if (
                                text.length > 100 &&
                                !/traceur|cookie|consentement|GDPR/i.test(text)
                            ) {
                                const sanitized = sanitizeToTextHtml(el);
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
                    crawlerLog.info(
                        `Saved job #${saved}: ${item.title} (${item.company || 'Unknown company'})`,
                    );
                } else {
                    crawlerLog.warning(`Missing title for DETAIL page: ${request.url}`);
                }
            } catch (err) {
                crawlerLog.error(`DETAIL handler error for ${request.url}: ${err.message}`);
            }
        },
    });

    // ---------------------------
    // Run hybrid flow
    // ---------------------------

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
        log.info('Phase 2: PlaywrightCrawler (DETAIL pages, JS-enabled)');
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
