export const CATEGORIES: Record<string, string> = {
  economics: 'Экономика',
  business: 'Компании',
  markets: 'Рынки',
  tech: 'Технологии',
  society: 'Общество',
};

export function categoryLabel(slug: string): string {
  return CATEGORIES[slug] ?? slug;
}
