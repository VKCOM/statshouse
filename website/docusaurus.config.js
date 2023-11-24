// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'StatsHouse',
  tagline: 'A highly-available, scalable, multi-tenant monitoring system',
  favicon: 'img/statshouse.ico',

  // Set the production url of your site here
  url: 'https://github.com',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/statshouse/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'VKCOM', // Usually your GitHub org/user name.
  projectName: 'statshouse', // Usually your repo name.
  deploymentBranch: 'gh-pages',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
//  i18n: {
//    defaultLocale: 'en',
//        locales: ['en', 'ru'],
//        path: 'i18n',
//        localeConfigs: {
//          en: {
//            label: 'English',
//            htmlLang: 'en-US',
//            path: 'en',
//          },
//          ru: {
//            label: 'Russian',
//            htmlLang: 'ru-RU',
//            path: 'ru',
//          },
//  },
//  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
//          editUrl:
//            'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
          routeBasePath: '/', // Serve the docs at the site's root
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/docusaurus-social-card.jpg',
      navbar: {
        title: 'StatsHouse',
        logo: {
          alt: 'StatsHouse Logo',
          src: 'img/statshouse.jpeg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Documentation',
          },
//          {to: '/blog', label: 'Blog', position: 'left'},
          {
            href: 'https://github.com/VKCOM/statshouse',
            label: 'GitHub',
            position: 'right',
          },
//          {
//            type: 'localeDropdown',
//            position: 'left',
//          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Documentation',
                to: '/introduction',
              },
            ],
          },
//          {
//            title: 'Community',
//            items: [
//              {
//                label: 'Stack Overflow',
//                href: 'https://stackoverflow.com/questions/tagged/docusaurus',
//              },
//              {
//                label: 'Discord',
//                href: 'https://discordapp.com/invite/docusaurus',
//              },
//              {
//                label: 'Twitter',
//                href: 'https://twitter.com/docusaurus',
//              },
//            ],
//          },
          {
            title: 'More',
            items: [
//              {
//                label: 'Blog',
//                to: '/blog',
//              },
              {
                label: 'GitHub',
                href: 'https://github.com/VKCOM/statshouse',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} StatsHouse.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
    }),
};

export default config;
