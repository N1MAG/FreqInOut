import { defineConfig } from "vitepress";

export default defineConfig({
  lang: "en-US",
  title: "FreqInOut",
  titleTemplate: ":title · FreqInOut",
  description:
    "Install, configure, and operate FreqInOut — a coordinated operations console for HF digital stations.",
  base: "/FreqInOut/",
  cleanUrls: true,
  lastUpdated: true,
  srcExclude: ["README.md"],
  ignoreDeadLinks: false,
  head: [
    ["link", { rel: "icon", type: "image/png", href: "/FreqInOut/fio-mark.png" }],
    ["meta", { name: "theme-color", content: "#12324a" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:title", content: "FreqInOut Documentation" }],
    [
      "meta",
      {
        property: "og:description",
        content: "Clear setup, operating, and troubleshooting guidance for FreqInOut.",
      },
    ],
  ],
  markdown: {
    theme: {
      light: "github-light",
      dark: "github-dark",
    },
  },
  themeConfig: {
    logo: "/fio-mark.png",
    siteTitle: "FreqInOut",
    nav: [
      { text: "Start here", link: "/start-here" },
      { text: "Install or upgrade", link: "/install/" },
      { text: "Set up a radio", link: "/guide/first-radio" },
      { text: "Mesh", link: "/integrations/mesh" },
      { text: "Support", link: "/support" },
    ],
    sidebar: [
      {
        text: "Start here",
        items: [
          { text: "Welcome to FIO", link: "/start-here" },
          { text: "Install or upgrade", link: "/install/" },
        ],
      },
      {
        text: "Set up your station",
        items: [{ text: "Configure your first radio", link: "/guide/first-radio" }],
      },
      {
        text: "Integrations",
        items: [{ text: "Local Mesh", link: "/integrations/mesh" }],
      },
      {
        text: "Help",
        items: [{ text: "Support and diagnostics", link: "/support" }],
      },
    ],
    search: {
      provider: "local",
      options: {
        detailedView: true,
      },
    },
    socialLinks: [
      { icon: "github", link: "https://github.com/N1MAG/FreqInOut" },
    ],
    editLink: {
      pattern: "https://github.com/N1MAG/FreqInOut/edit/main/website/:path",
      text: "Suggest a correction",
    },
    lastUpdated: {
      text: "Last reviewed",
      formatOptions: {
        dateStyle: "medium",
      },
    },
    outline: {
      level: [2, 3],
      label: "On this page",
    },
    docFooter: {
      prev: "Previous",
      next: "Next",
    },
    footer: {
      message: "Operator-focused documentation for FreqInOut.",
      copyright: "FreqInOut is licensed under GNU GPL v3.",
    },
    returnToTopLabel: "Return to top",
    sidebarMenuLabel: "Documentation menu",
    darkModeSwitchLabel: "Appearance",
  },
});
