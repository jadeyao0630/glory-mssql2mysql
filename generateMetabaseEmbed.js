const jwt = require('jsonwebtoken');

// Configuration
const METABASE_SITE_URL = 'http://cn.luyao.site:8003';
const METABASE_SECRET_KEY = 'e75554680b063742b10a71183c8fcc7b43a12524239058f849ca20f18dcec32f';

// Function to generate a signed embed URL for a Metabase dashboard
function generateMetabaseEmbedUrl(dashboardId) {
    const payload = {
        resource: { dashboard: dashboardId },
        params: {},
        exp: Math.round(Date.now() / 1000) + (10 * 60) // 10 minutes expiration
    };

    const token = jwt.sign(payload, METABASE_SECRET_KEY);

    const embedUrl = `${METABASE_SITE_URL}/embed/dashboard/${token}#bordered=true&titled=true`;
    return embedUrl;
}
// Example usage
const dashboardId = 5; // Replace with your actual dashboard ID
const embedUrl = generateMetabaseEmbedUrl(dashboardId);
console.log('Embed URL:', embedUrl);

