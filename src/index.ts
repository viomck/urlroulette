export interface Env {
    KV: KVNamespace;
    SECRET?: string;
    ALLOWED_ORIGIN: string;
}

export default {
    async fetch(
        request: Request,
        env: Env,
        _: ExecutionContext
    ): Promise<Response> {
        switch (request.method) {
            case "POST": return await handleCreate(env, request);
            case "GET": return await handleGet(env, request);
            default: return status(405);
        }
    },
};


async function handleCreate(env: Env, request: Request): Promise<Response> {
    if (
        env.SECRET && 
        request.headers.get("Authorization") !== `Secret ${env.SECRET}`
    ) {
        return status(401);
    }

    let { urlCount, urlPrefix } = await getUrlCountAndPrefix(env);
    const url = await request.text();

    if (!url.startsWith("http://") && !url.startsWith("https://")) {
        return status(400);
    }

    try {
        new URL(url);
    } catch (_: any) {
        return status(400);
    }

    const urlKey = `url.${urlPrefix}.${new Date().getTime()}`;

    await env.KV.put(urlKey, url);

    // store the key in case we want to delete later
    await env.KV.put(`urlKey.${encodeURIComponent(url)}`, urlKey);

    // KV can only list 1000 keys at a time, so in order to avoid a O(n) fetch
    // time / request count later (following cursor through pagination), we
    // divide urls into groups of 1000.  if I ever actually hit 1000 in prod
    // I'd be surprised, but it's good to future proof
    if (++urlCount === 1000) {
        await env.KV.put("urlPrefix", `${urlPrefix + 1}`);
        await env.KV.put("urlCount", "0");
    } else {
        await env.KV.put("urlCount", `${urlCount}`);
    }

    return status(201);
}

async function handleGet(env: Env, request: Request) {
    if (request.url.endsWith("/stats")) {
        return handleStats(env, request);
    }

    const { urlCount, urlPrefix } = await getUrlCountAndPrefix(env);

    // here, we estimate the total url count by adding:
    //  (the number of complete groups * 1000) +
    //  the number of urls in this group
    // I say estimate here because in the future I MAY remove URLs, and those
    // URLs MAY be in groups that aren't the current one, leaving them with 999
    // or so URLs.  this probaby will never happen, but good to be clear about
    // code.
    //
    // for example:
    // only one url group so far, and it has 20 urls.  this will be:
    //  (0*1000)+20 = 20
    // 2 full url groups, and another with 500 urls.  this will be:
    //  (2*1000)+500 = 2500
    const estimatedTotalUrlCount: number = (urlPrefix * 1000) + urlCount;

    // now we use estimatedTotalUrlCount to get a mostly fair random URL group
    // draw. if we were just to use 0-urlPrefix, newer URLs would have a heavy
    // bias in random draws.
    const estimatedTargetUrl = rand(estimatedTotalUrlCount);

    // now we can extrapolate a group from this
    const targetUrlPrefix = Math.floor(estimatedTargetUrl / 1000);

    // now we grab up to 1000 URLs from the group
    const results = await env.KV.list({ prefix: `url.${targetUrlPrefix}` });

    // finally, we can grab a random URL from this group
    return new Response(
        await env.KV.get(results.keys[rand(results.keys.length)].name),
        {
            headers: {
                "Access-Control-Allow-Origin": env.ALLOWED_ORIGIN
            }
        }
    );
}

async function handleStats(env: Env, request: Request) {
    if (
        env.SECRET && 
        request.headers.get("Authorization") !== `Secret ${env.SECRET}`
    ) {
        return status(401);
    }

    return new Response(
        JSON.stringify(
            await getUrlCountAndPrefix(env),
            undefined,
            4
        )
    );
}

async function getUrlCountAndPrefix(env: Env): Promise<
    { urlCount: number, urlPrefix: number }
> {
    const [urlCount, urlPrefix] = await Promise.all([
        getUrlCount(env), 
        getUrlPrefix(env)
    ]);

    return { urlCount, urlPrefix };
}

async function getUrlCount(env: Env): Promise<number> {
    return parseInt(await env.KV.get("urlCount") || "0");
}

async function getUrlPrefix(env: Env): Promise<number> {
    return parseInt(await env.KV.get("urlPrefix") || "0");
}

// draws a number between 0 and max, exclusive
function rand(max: number): number {
    return Math.floor(Math.random() * max);
}

function status(status: number): Response {
    return new Response(null, { status });
}
