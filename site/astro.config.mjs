// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const isProd = process.env.NODE_ENV === 'production';

// https://astro.build/config
export default defineConfig({
	site: 'https://tabidots.github.io',
	base: isProd ? '/sv-uttal' : '/', 
	trailingSlash: 'always',  // GitHub Pages prefers trailing slashes
	build: {
		format: 'directory',    // Creates /irregular/index.html instead of /irregular.html
	},
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
						{ label: 'Most frequent surprises', slug: 'irregular/frequent' },
					],
				},
				{
					label: 'Shifting accents',
					items: [
						{ label: 'Introduction', slug: 'accent' },
						{ label: 'Nouns', slug: 'accent/nouns' },
						{ label: 'Verbs', slug: 'accent/verbs' },
					],
				},
			],
			customCss: [
				'./src/styles/custom.css',
			],
			head: [
				// Basic Open Graph tags
				{
					tag: 'meta',
					attrs: { property: 'og:title', content: 'Swedish pronunciation notes • Anteckningar om svensk uttal' },
				},
				{
					tag: 'meta',
					attrs: {
						property: 'og:description', content: 'A reasonably complete guide for learners of Swedish covering the consonants that don\'t follow the rules, plus general tendencies regarding pitch accent.' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image', content: 'https://tabidots.github.io/sv-uttal/hero-og.png' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image:alt', content: 'Blue and yellow gradient lips representing Swedish pronunciation' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image:width', content: '1200' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image:height', content: '1200' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image:type', content: 'image/png' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:type', content: 'website' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:url', content: 'https://tabidots.github.io/sv-uttal/' },
				},

				// Twitter/X cards
				{
					tag: 'meta',
					attrs: { name: 'twitter:card', content: 'summary_large_image' },
				},
				{
					tag: 'meta',
					attrs: { name: 'twitter:title', content: 'Swedish pronunciation notes • Anteckningar om svensk uttal' },
				},
				{
					tag: 'meta',
					attrs: { name: 'twitter:description', content: 'A reasonably complete guide for learners of Swedish covering the consonants that don\'t follow the rules, plus general tendencies regarding pitch accent.' },
				},
				{
					tag: 'meta',
					attrs: { name: 'twitter:image', content: 'https://tabidots.github.io/sv-uttal/hero-og.png' },
				},
				{
					tag: 'meta',
					attrs: { name: 'twitter:image:alt', content: 'Blue and yellow gradient lips representing Swedish pronunciation' },
				},
			],
		}),
	],
});
