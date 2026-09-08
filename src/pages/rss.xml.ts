import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { marked } from 'marked';
import { categoryLabel } from '../lib/categories';

function escapeXml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function toRFC822(date: Date): string {
  return date.toUTCString().replace('GMT', '+0000');
}

export const GET: APIRoute = async ({ site }) => {
  const siteUrl = site ?? new URL('https://delovoy-vestnik.ru');
  const entries = (await getCollection('news')).sort(
    (a, b) => b.data.pubDate.valueOf() - a.data.pubDate.valueOf()
  );

  const items = await Promise.all(
    entries.map(async (entry) => {
      const url = new URL(`/news/${entry.id}/`, siteUrl).toString();
      const imageUrl = new URL(entry.data.image, siteUrl).toString();
      const fullTextHtml = await marked.parse(entry.body ?? '');

      return `
    <item>
      <title>${escapeXml(entry.data.title)}</title>
      <link>${escapeXml(url)}</link>
      <guid isPermaLink="true">${escapeXml(url)}</guid>
      <pubDate>${toRFC822(entry.data.pubDate)}</pubDate>
      <category>${escapeXml(categoryLabel(entry.data.category))}</category>
      <description><![CDATA[${entry.data.lead}]]></description>
      <enclosure url="${escapeXml(imageUrl)}" type="image/svg+xml" length="0" />
      <yandex:full-text><![CDATA[${fullTextHtml}]]></yandex:full-text>
    </item>`;
    })
  );

  const body = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:yandex="http://news.yandex.ru" xmlns:media="http://search.yahoo.com/mrss/">
  <channel>
    <title>Деловой Вестник</title>
    <link>${escapeXml(siteUrl.toString())}</link>
    <description>Новости экономики, политики и финансовых рынков.</description>
    <language>ru</language>
    <lastBuildDate>${toRFC822(new Date())}</lastBuildDate>${items.join('')}
  </channel>
</rss>`;

  return new Response(body, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
