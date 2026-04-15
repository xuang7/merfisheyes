/** @type {import('next').NextConfig} */
const nextConfig = {
  basePath: "/merfisheyes",
  output: "standalone",
  images: {
    remotePatterns: [
      { hostname: "lh3.googleusercontent.com" },
    ],
  },
};

module.exports = nextConfig;
