# Playwright's official image ships with Chromium + all its system deps
# pre-installed. We add ffmpeg on top so the videos worker can strip audio
# from downloaded mp4s before sending to Whisper.
FROM mcr.microsoft.com/playwright:v1.60.0-jammy

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

ENV NODE_ENV=production
WORKDIR /app

# Install npm deps first so Docker can cache this layer across code changes.
COPY package*.json ./
RUN npm install --omit=dev

# App code
COPY . .

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

EXPOSE 3000
CMD ["node", "server.js"]
