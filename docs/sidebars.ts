import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Get Started',
      collapsed: false,
      items: [
        'get-started/quickstart',
        'get-started/how-it-works',
        'get-started/glossary',
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      collapsed: false,
      items: [
        'guides/definitions',
        'guides/queries',
        'guides/lifecycle',
        'guides/deployment',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      items: [
        'reference/api',
        'reference/expressions',
        'reference/definition-schema',
        'reference/configuration',
      ],
    },
  ],
};

export default sidebars;
