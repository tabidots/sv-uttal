// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const isProd = process.env.NODE_ENV === 'production';

// https://astro.build/config
export default defineConfig({
	site: 'https://tabidots.github.io',
	base: isProd ? '/sv-uttal' : '/', 
	integrations: [
		starlight({
			title: 'Swedish pronunciation notes • Anteckningar om svensk uttal',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/tabidots/sv-uttal' }],
			sidebar: [
				{
					label: 'Surprise consonants',
					items: [
						{ label: 'Introduction', slug: 'irregular' },
						{ label: '-ion and eu-', slug: 'irregular/ion-eu' },
						{ label: 'sc, sch, and ch', slug: 'irregular/sc-sch-ch' },
						{ label: 'k and sk', slug: 'irregular/k-sk' },
						{ label: 'g and j', slug: 'irregular/g-j' },
					],
				},
				{
					label: 'Pitch Accent',
					items: [
						{ label: 'Introduction', slug: 'accent' },
					],
				},
			],
			customCss: [
				'./src/styles/custom.css',
			],
		}),
	],
});
