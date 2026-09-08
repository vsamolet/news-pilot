export const CATEGORIES: Record<string, string> = {
  economics: 'Экономика',
  business: 'Бизнес',
  markets: 'Финансы',
  tech: 'Технологии',
  society: 'Общество',
};

export function categoryLabel(slug: string): string {
  return CATEGORIES[slug] ?? slug;
}
