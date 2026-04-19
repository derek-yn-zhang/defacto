import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Defacto',
  tagline: 'Declarative entity state from event streams',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://derek-yn-zhang.github.io',
  baseUrl: '/defacto/',

  organizationName: 'derek-yn-zhang',
  projectName: 'defacto',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: '/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'Defacto',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/derek-yn-zhang/defacto',
          label: 'GitHub',
          position: 'right',
        },
        {
          href: 'https://pypi.org/project/defacto/',
          label: 'PyPI',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'light',
      copyright: `Defacto`,
    },
    prism: {
      theme: prismThemes.github,
      additionalLanguages: ['bash', 'yaml', 'toml', 'rust'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
