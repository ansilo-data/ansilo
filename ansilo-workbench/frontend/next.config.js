/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  trailingSlash: true,
  webpack: (config, { isServer }) => {
    // If in client, don't use fs module in npm
    if (!isServer) {
      config.resolve.fallback.fs = false
    }

    return config;
  },
}

module.exports = nextConfig
