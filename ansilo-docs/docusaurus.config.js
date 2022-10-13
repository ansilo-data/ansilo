// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require("prism-react-renderer/themes/github");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "Ansilo",
  tagline: "Build portals not pipelines",
  url: "https://docs.ansilo.io",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",
  // CloudFront Function will add trailing /index.html
  trailingSlash: true,
  favicon: "img/favicon.png",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "ansilo-data", // Usually your GitHub org/user name.
  projectName: "ansilo", // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
        },
        blog: {
          showReadingTime: true,
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
        gtag: {
          trackingID: "G-0BWGMZ7J2X",
          anonymizeIP: true,
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: "Ansilo",
        items: [
          {
            type: "doc",
            docId: "index",
            position: "left",
            label: "Documentation",
          },
          {
            href: "https://github.com/ansilo-data/ansilo",
            label: "GitHub",
            position: "right",
          },
        ],
      },
      footer: {
        style: "dark",
        links: [
          {
            title: "Docs",
            items: [
              {
                label: "Introduction",
                to: "/docs/",
              },
            ],
          },
          {
            title: "Community",
            items: [
              {
                label: "Stack Overflow",
                href: "https://stackoverflow.com/questions/tagged/ansilo",
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Ansilo. Built with Docusaurus.`,
      },
      colorMode: { defaultMode: "dark" },
      prism: {
        theme: require("prism-react-renderer/themes/vsLight"),
        darkTheme: require("prism-react-renderer/themes/vsDark"),
        additionalLanguages: ["yaml", "bash"],
      },
    }),
};

module.exports = config;
