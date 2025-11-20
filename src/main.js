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

        function findJobLinks($, base) {
            const links = new Set();
            // Updated selector strategy: look for any link that looks like a job offer
            $('a[href]').each((_, a) => {
                const href = $(a).attr('href');
                if (!href) return;
                // Matches /emplois/12345.html or similar patterns
                if (/\/emplois\/.*?\d+\.html/i.test(href)) {
                    const abs = toAbs(href, base);
                    if (abs) links.add(abs);
                }
            });
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
            },
            maxConcurrency: 10,
            requestHandlerTimeoutSecs: 60,
            // Use built-in header generation powered by got-scraping
            useHeaderGenerator: true,
            headerGeneratorOptions: {
                browsers: [
                    { name: "chrome", minVersion: 110 },
                    { name: "firefox", minVersion: 110 },
                    { name: "safari", minVersion: 16 }
                ],
                devices: ["desktop"],
                locales: ["fr-FR", "en-US"],
                operatingSystems: ["windows", "macos", "linux"],
            },
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    const links = findJobLinks($, request.url);
                    crawlerLog.info(`LIST [Page ${pageNo}] ${request.url} -> found ${links.length} links`);

                    if (links.length === 0) {
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

        await crawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
        log.info(`Finished. Saved ${saved} items`);
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });
