// Hellowork jobs scraper - CheerioCrawler implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, ProxyConfiguration } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

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

        const cleanText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript, iframe').remove();
            return $.root().text().replace(/\s+/g, ' ').trim();
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

        function extractFromJsonLd($) {
            const scripts = $('script[type="application/ld+json"]');
            for (let i = 0; i < scripts.length; i++) {
                try {
                    const parsed = JSON.parse($(scripts[i]).html() || '');
                    const arr = Array.isArray(parsed) ? parsed : [parsed];
                    for (const e of arr) {
                        if (!e) continue;
                        const t = e['@type'] || e.type;
                        if (t === 'JobPosting' || (Array.isArray(t) && t.includes('JobPosting'))) {
                            return {
                                title: e.title || e.name || null,
                                company: e.hiringOrganization?.name || null,
                                date_posted: e.datePosted || null,
                                description_html: e.description || null,
                                location: (e.jobLocation && e.jobLocation.address && (e.jobLocation.address.addressLocality || e.jobLocation.address.addressRegion)) || null,
                                salary: e.baseSalary?.value?.value || e.baseSalary?.value || null,
                                contract_type: e.employmentType || null,
                            };
                        }
                    }
                } catch (e) { /* ignore parsing errors */ }
            }
            return null;
        }

        function findJobLinks($, base, crawlerLog) {
            const links = new Set();

            // Log page title to verify we're on the right page
            const pageTitle = $('title').text();
            crawlerLog.info(`Page title: ${pageTitle}`);

            // Check for blocking elements
            const hasCookieBanner = $('[id*="cookie"], [class*="cookie"], [class*="consent"], [id*="consent"], #didomi').length > 0;
            if (hasCookieBanner) {
                crawlerLog.warning('Cookie/consent banner detected');
            }

            // Count total links for debugging
            const totalLinks = $('a[href]').length;
            crawlerLog.info(`Total links on page: ${totalLinks}`);

            // Check for "no results" message
            const noResultsText = $('body').text();
            if (/aucun.*résultat|no.*results|0.*offre/i.test(noResultsText) && totalLinks < 10) {
                crawlerLog.warning('Possible "no results" page detected');
            }

            // Multiple selector strategies for job links
            const selectors = [
                'a[href*="/emplois/"]',
                'a[href*="/emploi/"]',
                'a[data-cy*="job"]',
                'a[class*="job"]',
                '.job-list a',
                '[class*="offer"] a',
                '[class*="offre"] a'
            ];

            for (const selector of selectors) {
                $(selector).each((_, a) => {
                    const href = $(a).attr('href');
                    if (!href) return;
                    if (/\/emplois?\/.*?\d+\.html/i.test(href)) {
                        const abs = toAbs(href, base);
                        if (abs && abs.includes('hellowork.com')) links.add(abs);
                    }
                });
            }

            // Fallback: any link with job ID pattern
            if (links.size === 0) {
                crawlerLog.warning('Primary selectors found 0 links, trying fallback');
                $('a[href]').each((_, a) => {
                    const href = $(a).attr('href');
                    if (!href) return;
                    if (/\/emplois?\/\d+\.html/i.test(href)) {
                        const abs = toAbs(href, base);
                        if (abs && abs.includes('hellowork.com')) {
                            links.add(abs);
                            if (links.size <= 3) crawlerLog.info(`Found job link: ${abs}`);
                        }
                    }
                });
            }

            return [...links];
        }

        function findNextPage($, base) {
            const url = new URL(base);
            const currentPage = parseInt(url.searchParams.get('p') || '1');
            // Check if there's a "next" button or if we just blindly increment
            // Hellowork usually has a pagination block. If we can't find it, we might stop.
            // But blindly incrementing is risky if we don't check for "no results".
            // Let's check for a "next" link or blindly increment if we found results.

            // If we found 0 links on this page, we probably shouldn't paginate further.
            // This logic is handled in the requestHandler.

            url.searchParams.set('p', (currentPage + 1).toString());
            return url.href;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 5,
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: 50,
                sessionOptions: {
                    maxErrorScore: 5,
                    errorScoreDecrement: 0.5,
                    maxUsageCount: 50,
                },
            },
            maxConcurrency: 5, // Reduced to avoid rate limiting
            requestHandlerTimeoutSecs: 90,
            maxRequestsPerMinute: 60,
            // Use built-in header generation with French preferences
            useHeaderGenerator: true,
            headerGeneratorOptions: {
                browsers: [
                    { name: "chrome", minVersion: 115 },
                    { name: "firefox", minVersion: 115 }
                ],
                devices: ["desktop"],
                locales: ["fr-FR"],
                operatingSystems: ["windows", "macos"],
                httpVersion: "2",
            },
            additionalMimeTypes: ['text/html', 'application/xhtml+xml'],
            ignoreSslErrors: false,
            maxRequestsPerCrawl: MAX_PAGES * 30,
            preNavigationHooks: [
                async ({ request, session }) => {
                    // Set comprehensive cookies to bypass consent banners and tracking
                    if (session) {
                        const cookieDomain = '.hellowork.com';
                        const cookies = [
                            { name: 'euconsent-v2', value: 'CPzYB4APzYB4AAHABBFRDECsAP_AAAAAAAYgJNpB9G7WTXFneXp2cP0EIYRlxxL2HjTCpBo6gFFAWJAgFIDUCQEAAD0ACREAACgBRAAQAKAgEAKBoAQEEBAoKAAAgCAoQQBB4AgEAABBQAAEIASEAQgACAAmAAAAASgAAAAACAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAA', domain: cookieDomain },
                            { name: 'didomi_token', value: 'eyJleHBpcmVzIjoxNzAwMDAwMDAwfQ==', domain: cookieDomain },
                            { name: 'cookie_consent', value: 'accepted', domain: cookieDomain },
                            { name: 'gdpr_consent', value: 'true', domain: cookieDomain },
                            { name: 'cconsent', value: 'all', domain: cookieDomain },
                            { name: 'consent_marketing', value: '1', domain: cookieDomain },
                            { name: '_ga', value: 'GA1.2.123456789.1700000000', domain: cookieDomain },
                            { name: '_gid', value: 'GA1.2.987654321.1700000000', domain: cookieDomain }
                        ];
                        session.setCookies(cookies, request.url);
                    }
                }
            ],
            failedRequestHandler: async ({ request, error }, context) => {
                log.error(`Request ${request.url} failed ${request.retryCount} times: ${error.message}`);
                log.error(`Error stack: ${error.stack}`);
            },
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    crawlerLog.info(`Processing LIST page ${pageNo}: ${request.url}`);

                    // Enhanced debugging
                    const h1Text = $('h1').text().trim();
                    const offersCount = $('body').text().match(/(\d+[\s,]?\d*)\s*offre/i)?.[1];
                    crawlerLog.info(`Page H1: ${h1Text}`);
                    if (offersCount) crawlerLog.info(`Page shows ${offersCount} offers`);

                    // Check for captcha or blocking
                    const bodyHtml = $('body').html() || '';
                    if (/captcha|blocked|robot|recaptcha/i.test(bodyHtml)) {
                        crawlerLog.error('Possible CAPTCHA or blocking detected!');
                    }

                    const links = findJobLinks($, request.url, crawlerLog);
                    crawlerLog.info(`LIST [Page ${pageNo}] -> found ${links.length} job links`);

                    // Enhanced debugging for empty results
                    if (links.length === 0) {
                        const bodyText = $('body').text().substring(0, 800);
                        const htmlSnippet = bodyHtml.substring(0, 1000);
                        crawlerLog.warning(`No job links found. Body text preview: ${bodyText}`);
                        crawlerLog.warning(`HTML snippet: ${htmlSnippet}`);
                        
                        // Check if we're on the right page
                        if (!bodyText.includes('offre') && !bodyText.includes('emploi')) {
                            crawlerLog.error('Page does not contain expected French job keywords');
                        }
                    }

                    if (links.length === 0 && pageNo === 1) {
                        crawlerLog.error(`CRITICAL: No links found on first page. Search might be blocked or URL invalid.`);
                        // Don't return immediately - let it try pagination once to see if page 2 works
                    }

                    if (links.length === 0 && pageNo > 1) {
                        crawlerLog.warning(`No links found on page ${pageNo}. Stopping pagination.`);
                        return;
                    }

                    if (collectDetails) {
                        const remaining = RESULTS_WANTED - saved;
                        const toEnqueue = links.slice(0, Math.max(0, remaining));
                        if (toEnqueue.length) await enqueueLinks({ urls: toEnqueue, userData: { label: 'DETAIL' } });
                    } else {
                        const remaining = RESULTS_WANTED - saved;
                        const toPush = links.slice(0, Math.max(0, remaining));
                        if (toPush.length) { await Dataset.pushData(toPush.map(u => ({ url: u, _source: 'hellowork.com' }))); saved += toPush.length; }
                    }

                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url);
                        if (next) await enqueueLinks({ urls: [next], userData: { label: 'LIST', pageNo: pageNo + 1 } });
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    try {
                        const json = extractFromJsonLd($);
                        const data = json || {};

                        // Fallbacks
                        if (!data.title) data.title = $('h1').first().text().trim() || $('[class*="title"]').first().text().trim() || null;

                        if (!data.company) {
                            const companyLink = $('h1 a').first();
                            if (companyLink.length) {
                                data.company = companyLink.text().trim();
                            } else {
                                // Try to find company in other common places
                                data.company = $('[class*="company"]').first().text().trim() ||
                                    $('[class*="entreprise"]').first().text().trim() || null;
                            }
                        }

                        if (!data.description_html) {
                            const descSections = ['.job-description', '[class*="mission"]', '[class*="profil"]', '[class*="description"]', 'section[data-v-step="description"]'];
                            let descHtml = '';
                            for (const sel of descSections) {
                                const section = $(sel);
                                if (section.length) descHtml += section.html();
                            }
                            data.description_html = descHtml || null;
                        }
                        data.description_text = data.description_html ? cleanText(data.description_html) : null;

                        if (!data.location) {
                            data.location = $('[class*="location"]').first().text().trim() ||
                                $('li:contains("Localisation")').text().replace('Localisation', '').trim() || null;
                        }

                        if (!data.date_posted) {
                            const dateMatch = $('body').text().match(/Publiée le (\d{2}\/\d{2}\/\d{4})/);
                            data.date_posted = dateMatch ? dateMatch[1] : null;
                        }

                        // Extract salary and contract type if not in JSON-LD
                        if (!data.salary) {
                            const salaryMatch = $('body').text().match(/(\d+(?:\s?\d+)*(?:,\d+)?\s?€\s?\/\s?(?:mois|an))/);
                            data.salary = salaryMatch ? salaryMatch[1].trim() : null;
                        }

                        if (!data.contract_type) {
                            const contractMatch = $('body').text().match(/(CDI|CDD|Stage|Intérim|Temps plein|Temps partiel)/);
                            data.contract_type = contractMatch ? contractMatch[1] : null;
                        }

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

                        if (item.title) { // Only push if we at least found a title
                            await Dataset.pushData(item);
                            saved++;
                        } else {
                            crawlerLog.warning(`DETAIL ${request.url} -> Could not extract title, skipping.`);
                        }
                    } catch (err) { crawlerLog.error(`DETAIL ${request.url} failed: ${err.message}`); }
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
