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
						{ label: 'Introduction', link: '/irregular/' },
						{ label: '-ion and eu-', link: '/irregular/ion-eu/' },
						{ label: 'sc, sch, and ch', link: '/irregular/sc-sch-ch/' },
						{ label: 'k and sk', link: '/irregular/k-sk/' },
						{ label: 'g and j', link: '/irregular/g-j/' },
					],
				},
				{
					label: 'Pitch Accent',
					items: [
						{ label: 'Introduction', link: '/accent/' },
					],
				  },
			],
			customCss: [
				'./src/styles/custom.css',
			],
		}),
	],
});
