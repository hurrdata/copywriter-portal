This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Extra Space Truth Engine (Scripts)

This project includes a suite of Python scripts used to generate and sync AI copy directly to the database.

### Setup
1. Create a virtual environment: `python -m venv venv`
2. Activate it: `source venv/bin/activate` (Mac/Linux) or `venv\Scripts\activate` (Windows)
3. Install dependencies: `pip install -r scripts/requirements.txt`
4. Set up your `.env` file based on `.env.example`.

### Core Scripts
- **`scripts/generate_copy_batch.py`**: The main generation engine. It uses Gemini 2.0/2.5 to draft copy based on Segment Strategy and local POI data.
- **`scripts/sync_zip_customer_mix.py`**: Syncs granular ZIP customer data from Excel into the database.

### Running Generation
To refresh a batch of store copy:
```bash
python scripts/generate_copy_batch.py
```
*Note: Ensure your `output_final.xlsx` and expansion files are in the repository root (ignored by git).*

## Learn More
... (existing learn more content)
