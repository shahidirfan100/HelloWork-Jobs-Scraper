// Fast, stealthy HelloWork scraper: HTTP-first with JSON-LD extraction
import { Actor, log } from 'apify';
import { CheerioCrawler, PlaywrightCrawler, Dataset } from 'crawlee';

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

// ---------- JSON-LD EXTRACTION (PRIMARY METHOD - FULL DETAILS) ----------

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
                    let city = null;
                    let postalCode = null;
                    let region = null;
                    let country = null;

                    if (block.jobLocation) {
                        const jl = Array.isArray(block.jobLocation) ? block.jobLocation[0] : block.jobLocation;
                        const addr = jl?.address || {};
                        city = addr.addressLocality || null;
                        postalCode = addr.postalCode || null;
                        region = addr.addressRegion || null;
                        country = addr.addressCountry || null;
                        const parts = [city, postalCode, region, country].filter(Boolean);
                        if (parts.length) location = parts.join(', ');
                    }

                    // Extract salary details
                    let salary = null;
                    let salaryMin = null;
                    let salaryMax = null;
                    let salaryCurrency = null;
                    let salaryPeriod = null;

                    if (block.baseSalary) {
                        const bs = block.baseSalary;
                        salaryCurrency = bs.currency || '€';
                        if (bs.value) {
                            const val = bs.value;
                            if (typeof val === 'object') {
                                salaryMin = val.minValue || null;
                                salaryMax = val.maxValue || null;
                                salaryPeriod = val.unitText || null;
                                if (salaryMin && salaryMax) {
                                    salary = `${salaryMin} - ${salaryMax} ${salaryCurrency}`;
                                } else if (salaryMin || salaryMax) {
                                    salary = `${salaryMin || salaryMax} ${salaryCurrency}`;
                                }
                            } else {
                                salary = `${val} ${salaryCurrency}`;
                            }
                        }
                        if (salaryPeriod) {
                            salary = salary ? `${salary} / ${salaryPeriod}` : null;
                        }
                    }

                    // Extract company details
                    let company = null;
                    let companyUrl = null;
                    let companyLogo = null;

                    if (block.hiringOrganization) {
                        const org = block.hiringOrganization;
                        company = typeof org === 'string' ? org : org.name || null;
                        companyUrl = org.sameAs || org.url || null;
                        if (org.logo) {
                            companyLogo = typeof org.logo === 'string' ? org.logo : org.logo.url || null;
                        }
                    }

                    // Extract description (clean HTML)
                    let descriptionHtml = block.description || null;
                    let descriptionText = null;
                    if (descriptionHtml) {
                        descriptionText = descriptionHtml.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
                    }

                    // Extract contract/employment type
                    let contractType = null;
                    let employmentTypeRaw = null;

                    if (block.employmentType) {
                        const et = Array.isArray(block.employmentType) ? block.employmentType : [block.employmentType];
                        employmentTypeRaw = et.join(', ');
                        const typeMap = {
                            'FULL_TIME': 'CDI',
                            'PART_TIME': 'Temps partiel',
                            'CONTRACTOR': 'Freelance',
                            'TEMPORARY': 'CDD',
                            'INTERN': 'Stage',
                            'APPRENTICESHIP': 'Alternance',
                        };
                        contractType = et.map(t => typeMap[t] || t).join(', ');
                    }

                    // Extract skills
                    let skills = null;
                    if (block.skills) {
                        skills = Array.isArray(block.skills) ? block.skills.join(', ') : block.skills;
                    }

                    // Extract qualifications/education
                    let qualifications = null;
                    let educationRequirements = null;
                    let experienceRequirements = null;

                    if (block.qualifications) {
                        qualifications = Array.isArray(block.qualifications)
                            ? block.qualifications.join(', ')
                            : block.qualifications;
                    }
                    if (block.educationRequirements) {
                        const edu = block.educationRequirements;
                        educationRequirements = typeof edu === 'string' ? edu : edu.credentialCategory || null;
                    }
                    if (block.experienceRequirements) {
                        const exp = block.experienceRequirements;
                        experienceRequirements = typeof exp === 'string' ? exp : exp.monthsOfExperience
                            ? `${exp.monthsOfExperience} months` : null;
                    }

                    // Extract job benefits
                    let benefits = null;
                    if (block.jobBenefits) {
                        benefits = Array.isArray(block.jobBenefits)
                            ? block.jobBenefits.join(', ')
                            : block.jobBenefits;
                    }

                    // Extract industry/category
                    let industry = null;
                    if (block.industry) {
                        industry = Array.isArray(block.industry) ? block.industry.join(', ') : block.industry;
                    }

                    // Extract work settings (remote, hybrid, onsite)
                    let workSetting = null;
                    if (block.jobLocationType) {
                        workSetting = block.jobLocationType;
                    }
                    if (block.applicantLocationRequirements) {
                        workSetting = workSetting ? `${workSetting} (remote eligible)` : 'Remote eligible';
                    }

                    // Extract validity period
                    let validThrough = block.validThrough || null;

                    // Extract identifier/reference
                    let jobId = null;
                    if (block.identifier) {
                        const id = block.identifier;
                        jobId = typeof id === 'string' ? id : id.value || null;
                    }

                    return {
                        // Core fields
                        title: cleanText(block.title) || null,
                        company: cleanText(company) || null,
                        location: cleanText(location) || null,
                        salary: cleanText(salary) || null,
                        contract_type: cleanText(contractType) || null,
                        date_posted: block.datePosted || null,

                        // Extended location details
                        city: cleanText(city) || null,
                        postal_code: cleanText(postalCode) || null,
                        region: cleanText(region) || null,
                        country: cleanText(country) || null,

                        // Extended salary details
                        salary_min: salaryMin,
                        salary_max: salaryMax,
                        salary_currency: salaryCurrency,
                        salary_period: salaryPeriod,

                        // Company details
                        company_url: companyUrl,
                        company_logo: companyLogo,

                        // Description
                        description_html: descriptionHtml,
                        description_text: cleanText(descriptionText) || null,

                        // Requirements
                        skills: cleanText(skills) || null,
                        qualifications: cleanText(qualifications) || null,
                        education: cleanText(educationRequirements) || null,
                        experience: cleanText(experienceRequirements) || null,

                        // Additional details
                        benefits: cleanText(benefits) || null,
                        industry: cleanText(industry) || null,
                        work_setting: cleanText(workSetting) || null,
                        employment_type_raw: employmentTypeRaw,
                        valid_through: validThrough,
                        job_id: cleanText(jobId) || null,

                        // Metadata
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
    const BATCH_SIZE = 10; // Push data every N items

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
    const seenUrls = new Set();
    const jobBatch = []; // Buffer for batch pushing

    // ---------- BATCH PUSH HELPER ----------

    async function pushBatch(force = false) {
        if (jobBatch.length >= BATCH_SIZE || (force && jobBatch.length > 0)) {
            const batchToPush = jobBatch.splice(0, jobBatch.length);
            await Dataset.pushData(batchToPush);
            log.info(`📦 Pushed batch of ${batchToPush.length} jobs (total saved: ${saved})`);
        }
    }

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

    function findJobLinksCheerio($) {
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
        async requestHandler({ request, $, enqueueLinks }) {
            const pageNo = request.userData?.pageNo || 1;
            await randomDelay(200, 600);

            const links = findJobLinksCheerio($);

            if (links.length === 0 && pageNo > 1) {
                return; // End of pagination
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

    const playwrightFallbackUrls = [];

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
        async requestHandler({ request, $ }) {
            if (saved >= RESULTS_WANTED) return;
            if (Date.now() - startTime > HARD_TIME_LIMIT_MS) return;

            await randomDelay(150, 500);

            const pageUrl = request.url;

            // Priority 1: Try JSON-LD extraction
            let jobData = extractJobFromJsonLd($, pageUrl);

            // Priority 2: Fallback to HTML parsing
            if (!jobData || !jobData.title) {
                jobData = extractJobFromHtml($, pageUrl);
            }

            // Validate and add to batch
            if (jobData && jobData.title) {
                jobBatch.push(jobData);
                saved++;

                // Push batch when it reaches BATCH_SIZE
                await pushBatch();
            } else {
                // Add to Playwright fallback queue
                playwrightFallbackUrls.push(pageUrl);
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.warning(`DETAIL failed: ${error.message}`);
            playwrightFallbackUrls.push(request.url);
        },
    });

    // ---------- PlaywrightCrawler (FALLBACK for blocked pages) ----------

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
        async requestHandler({ request, page }) {
            if (saved >= RESULTS_WANTED) return;

            try {
                // Dismiss cookie/country modals
                try {
                    await page.click('#didomi-notice-agree-button', { timeout: 2000 });
                } catch { /* ignore */ }
                try {
                    const selectExists = await page.$('select');
                    if (selectExists) {
                        await page.selectOption('select', { label: 'France' });
                        await page.click('button:has-text("OK")', { timeout: 2000 });
                    }
                } catch { /* ignore */ }

                await page.waitForSelector('h1', { timeout: 8000 }).catch(() => { });

                const html = await page.content();
                const cheerio = await import('cheerio');
                const $ = cheerio.load(html);

                let jobData = extractJobFromJsonLd($, request.url);
                if (!jobData || !jobData.title) {
                    jobData = extractJobFromHtml($, request.url);
                    if (jobData) jobData._source = 'playwright-html';
                } else {
                    jobData._source = 'playwright-jsonld';
                }

                if (jobData && jobData.title) {
                    jobBatch.push(jobData);
                    saved++;
                    await pushBatch();
                }
            } catch (err) {
                log.error(`Playwright error: ${err.message}`);
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.error(`Playwright failed: ${error.message}`);
        },
    });

    // ---------- RUN FLOW ----------

    log.info('=== HelloWork Scraper (HTTP-First + JSON-LD) ===');
    log.info(`Target: ${RESULTS_WANTED} jobs | Max pages: ${MAX_PAGES}`);

    // Phase 1: Collect job URLs
    log.info('📋 Phase 1: Collecting job URLs...');
    await listCrawler.run(
        initialUrls.map((u) => ({ url: u, userData: { pageNo: 1 } })),
    );

    const detailArray = Array.from(detailUrls);
    log.info(`✓ Found ${detailArray.length} job URLs`);

    // Phase 2: Extract job data via HTTP
    if (collectDetails && detailArray.length > 0) {
        log.info('⚡ Phase 2: Extracting job data...');
        await detailCrawler.run(detailArray.map((u) => ({ url: u })));

        // Push any remaining jobs in batch
        await pushBatch(true);

        // Phase 3: Playwright fallback if needed
        if (playwrightFallbackUrls.length > 0 && saved < RESULTS_WANTED) {
            log.info(`🎭 Phase 3: Fallback for ${playwrightFallbackUrls.length} pages...`);
            await playwrightCrawler.run(playwrightFallbackUrls.map((u) => ({ url: u })));
            await pushBatch(true);
        }
    } else if (!collectDetails) {
        for (const u of detailArray.slice(0, RESULTS_WANTED - saved)) {
            jobBatch.push({ url: u, _source: 'list-only' });
            saved++;
        }
        await pushBatch(true);
    }

    // Summary
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    log.info('=== COMPLETED ===');
    log.info(`✓ Jobs: ${saved} | Time: ${elapsed}s | Speed: ${(saved / parseFloat(elapsed) || 0).toFixed(2)}/sec`);

    if (saved === 0) {
        log.warning('No jobs scraped. Check query or site changes.');
    }
});
