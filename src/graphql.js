import dotenv from "dotenv";

// Load environment variables from .env file
dotenv.config();

export async function graphqlQuery(query) {
  return fetch(
    `${process.env.AURORA_ENDPOINT}?x-api-key=${process.env.AURORA_API_KEY}`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query,
      }),
      redirect: "follow",
    }
  ).then((response) => response.json());
}
