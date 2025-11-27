let KV_BINDING;
let DB;
const banPath = [
  'login', 'admin', '__total_count',
  'admin.html', 'login.html',
  'daisyui@5.css', 'tailwindcss@4.js',
  'qr-code-styling.js', 'zxing.js',
  'robots.txt', 'wechat.svg',
  'favicon.svg',
];

// 数据库初始化
async function initDatabase() {
  try {
    // 创建表
    await DB.prepare(`
      CREATE TABLE IF NOT EXISTS mappings (
        path TEXT PRIMARY KEY,
        target TEXT NOT NULL,
        name TEXT,
        expiry TEXT,
        enabled INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        isWechat INTEGER DEFAULT 0,
        qrCodeData TEXT,
        imageUrl TEXT,
        imageBase64 TEXT,
        imageAlt TEXT
      )
    `).run();

    // 检查是否需要添加新列
    const tableInfo = await DB.prepare("PRAGMA table_info(mappings)").all();
    const columns = tableInfo.results?.map(col => col.name) || [];

    // 添加 isWechat 列（如果不存在）
    if (!columns.includes('isWechat')) {
      await DB.prepare(`ALTER TABLE mappings ADD COLUMN isWechat INTEGER DEFAULT 0`).run();
    }

    // 添加 qrCodeData 列（如果不存在）
    if (!columns.includes('qrCodeData')) {
      await DB.prepare(`ALTER TABLE mappings ADD COLUMN qrCodeData TEXT`).run();
    }

    // 添加图片相关列（如果不存在）
    if (!columns.includes('imageUrl')) {
      await DB.prepare(`ALTER TABLE mappings ADD COLUMN imageUrl TEXT`).run();
    }
    if (!columns.includes('imageBase64')) {
      await DB.prepare(`ALTER TABLE mappings ADD COLUMN imageBase64 TEXT`).run();
    }
    if (!columns.includes('imageAlt')) {
      await DB.prepare(`ALTER TABLE mappings ADD COLUMN imageAlt TEXT`).run();
    }

    // 添加索引
    await DB.prepare(`CREATE INDEX IF NOT EXISTS idx_expiry ON mappings(expiry)`).run();
    await DB.prepare(`CREATE INDEX IF NOT EXISTS idx_created_at ON mappings(created_at)`).run();
    await DB.prepare(`CREATE INDEX IF NOT EXISTS idx_enabled_expiry ON mappings(enabled, expiry)`).run();
  } catch (error) {
    console.error('数据库初始化失败:', error);
    throw error;
  }
}

// Cookie 相关函数
function verifyAuthCookie(request, env) {
  try {
    const cookie = request.headers.get('Cookie') || '';
    const authToken = cookie.split(';').find(c => c.trim().startsWith('token='));
    if (!authToken) return false;
    const token = authToken.split('=')[1]?.trim();
    return token === env.PASSWORD;
  } catch (error) {
    console.error('Cookie验证失败:', error);
    return false;
  }
}

function setAuthCookie(password) {
  const isProd = request.url.startsWith('https://');
  return {
    'Set-Cookie': `token=${password}; Path=/; HttpOnly; SameSite=Lax; ${isProd ? 'Secure;' : ''} Max-Age=86400`,
    'Content-Type': 'application/json'
  };
}

function clearAuthCookie() {
  return {
    'Set-Cookie': 'token=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0',
    'Content-Type': 'application/json'
  };
}

// 数据库操作相关函数
async function listMappings(page = 1, pageSize = 10) {
  const offset = (page - 1) * pageSize;
  
  try {
    const results = await DB.prepare(`
      WITH filtered_mappings AS (
        SELECT * FROM mappings 
        WHERE path NOT IN (${banPath.map(() => '?').join(',')})
      )
      SELECT 
        filtered.*,
        (SELECT COUNT(*) FROM filtered_mappings) as total_count
      FROM filtered_mappings as filtered
      ORDER BY created_at DESC
      LIMIT ? OFFSET ?
    `).bind(...banPath, pageSize, offset).all();

    if (!results.results || results.results.length === 0) {
      return {
        mappings: {},
        total: 0,
        page,
        pageSize,
        totalPages: 0
      };
    }

    const total = results.results[0].total_count;
    const mappings = {};

    for (const row of results.results) {
      mappings[row.path] = {
        target: row.target,
        name: row.name,
        expiry: row.expiry,
        enabled: row.enabled === 1,
        isWechat: row.isWechat === 1,
        qrCodeData: row.qrCodeData,
        imageUrl: row.imageUrl,
        imageBase64: row.imageBase64,
        imageAlt: row.imageAlt
      };
    }

    return {
      mappings,
      total,
      page,
      pageSize,
      totalPages: Math.ceil(total / pageSize)
    };
  } catch (error) {
    console.error('获取映射列表失败:', error);
    throw error;
  }
}

async function createMapping(path, target, name, expiry, enabled = true, isWechat = false, qrCodeData = null, imageUrl = null, imageBase64 = null, imageAlt = null) {
  if (!path || !target || typeof path !== 'string' || typeof target !== 'string') {
    throw new Error('无效的输入参数');
  }

  if (banPath.includes(path)) {
    throw new Error('该短链名已被系统保留，请使用其他名称');
  }

  if (expiry && isNaN(Date.parse(expiry))) {
    throw new Error('无效的过期日期');
  }

  if (isWechat && !qrCodeData) {
    throw new Error('微信二维码必须提供原始二维码数据');
  }

  try {
    await DB.prepare(`
      INSERT INTO mappings (path, target, name, expiry, enabled, isWechat, qrCodeData, imageUrl, imageBase64, imageAlt)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      path,
      target,
      name || null,
      expiry || null,
      enabled ? 1 : 0,
      isWechat ? 1 : 0,
      qrCodeData,
      imageUrl,
      imageBase64,
      imageAlt
    ).run();
  } catch (error) {
    console.error('创建映射失败:', error);
    throw error;
  }
}

async function deleteMapping(path) {
  if (!path || typeof path !== 'string') {
    throw new Error('无效的输入参数');
  }

  if (banPath.includes(path)) {
    throw new Error('系统保留的短链名无法删除');
  }

  try {
    await DB.prepare('DELETE FROM mappings WHERE path = ?').bind(path).run();
  } catch (error) {
    console.error('删除映射失败:', error);
    throw error;
  }
}

async function updateMapping(originalPath, newPath, target, name, expiry, enabled = true, isWechat = false, qrCodeData = null, imageUrl = null, imageBase64 = null, imageAlt = null) {
  if (!originalPath || !newPath || !target) {
    throw new Error('无效的输入参数');
  }

  if (banPath.includes(newPath)) {
    throw new Error('该短链名已被系统保留，请使用其他名称');
  }

  if (expiry && isNaN(Date.parse(expiry))) {
    throw new Error('无效的过期日期');
  }

  try {
    // 获取原有数据
    const existingMapping = await DB.prepare(`
      SELECT qrCodeData, imageUrl, imageBase64, imageAlt
      FROM mappings
      WHERE path = ?
    `).bind(originalPath).first();

    if (!existingMapping) {
      throw new Error('原始映射不存在');
    }

    // 使用原有数据（如果未提供新数据）
    if (!qrCodeData && isWechat) {
      qrCodeData = existingMapping.qrCodeData;
    }
    if (imageUrl === undefined) imageUrl = existingMapping.imageUrl;
    if (imageBase64 === undefined) imageBase64 = existingMapping.imageBase64;
    if (imageAlt === undefined) imageAlt = existingMapping.imageAlt;

    if (isWechat && !qrCodeData) {
      throw new Error('微信二维码必须提供原始二维码数据');
    }

    await DB.prepare(`
      UPDATE mappings 
      SET path = ?, target = ?, name = ?, expiry = ?, enabled = ?, isWechat = ?, qrCodeData = ?,
          imageUrl = ?, imageBase64 = ?, imageAlt = ?
      WHERE path = ?
    `).bind(
      newPath,
      target,
      name || null,
      expiry || null,
      enabled ? 1 : 0,
      isWechat ? 1 : 0,
      qrCodeData,
      imageUrl,
      imageBase64,
      imageAlt,
      originalPath
    ).run();
  } catch (error) {
    console.error('更新映射失败:', error);
    throw error;
  }
}

async function getExpiringMappings() {
  try {
    const today = new Date();
    today.setHours(23, 59, 59, 999);
    const now = today.toISOString();
    
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const dayStart = todayStart.toISOString();
    
    const threeDaysFromNow = new Date(todayStart);
    threeDaysFromNow.setDate(todayStart.getDate() + 3);
    threeDaysFromNow.setHours(23, 59, 59, 999);
    const threeDaysLater = threeDaysFromNow.toISOString();

    const results = await DB.prepare(`
      WITH categorized_mappings AS (
        SELECT 
          path, name, target, expiry, enabled, isWechat, qrCodeData,
          imageUrl, imageBase64, imageAlt,
          CASE 
            WHEN datetime(expiry) < datetime(?) THEN 'expired'
            WHEN datetime(expiry) <= datetime(?) THEN 'expiring'
          END as status
        FROM mappings 
        WHERE expiry IS NOT NULL 
          AND datetime(expiry) <= datetime(?) 
          AND enabled = 1
      )
      SELECT * FROM categorized_mappings
      ORDER BY expiry ASC
    `).bind(dayStart, threeDaysLater, threeDaysLater).all();

    const mappings = {
      expiring: [],
      expired: []
    };
    
    for (const row of results.results) {
      const mapping = {
        path: row.path,
        name: row.name,
        target: row.target,
        expiry: row.expiry,
        enabled: row.enabled === 1,
        isWechat: row.isWechat === 1,
        qrCodeData: row.qrCodeData,
        imageUrl: row.imageUrl,
        imageBase64: row.imageBase64,
        imageAlt: row.imageAlt
      };

      if (row.status === 'expired') {
        mappings.expired.push(mapping);
      } else {
        mappings.expiring.push(mapping);
      }
    }

    return mappings;
  } catch (error) {
    console.error('获取过期映射失败:', error);
    throw error;
  }
}

async function cleanupExpiredMappings(batchSize = 100) {
  try {
    const now = new Date().toISOString();
    
    while (true) {
      const batch = await DB.prepare(`
        SELECT path 
        FROM mappings 
        WHERE expiry IS NOT NULL 
          AND expiry < ? 
        LIMIT ?
      `).bind(now, batchSize).all();

      if (!batch.results || batch.results.length === 0) {
        break;
      }

      const paths = batch.results.map(row => row.path);
      const placeholders = paths.map(() => '?').join(',');
      await DB.prepare(`
        DELETE FROM mappings 
        WHERE path IN (${placeholders})
      `).bind(...paths).run();

      if (batch.results.length < batchSize) {
        break;
      }
    }
  } catch (error) {
    console.error('清理过期映射失败:', error);
    throw error;
  }
}

async function migrateFromKV() {
  try {
    let cursor = null;
    do {
      const listResult = await KV_BINDING.list({ cursor, limit: 1000 });
      
      for (const key of listResult.keys) {
        if (!banPath.includes(key.name)) {
          const value = await KV_BINDING.get(key.name, { type: "json" });
          if (value) {
            try {
              await createMapping(
                key.name,
                value.target,
                value.name,
                value.expiry,
                value.enabled,
                value.isWechat,
                value.qrCodeData,
                value.imageUrl || null,
                value.imageBase64 || null,
                value.imageAlt || null
              );
            } catch (e) {
              console.error(`迁移失败 ${key.name}:`, e);
            }
          }
        }
      }
      
      cursor = listResult.cursor;
    } while (cursor);
  } catch (error) {
    console.error('从KV迁移数据失败:', error);
    throw error;
  }
}

export default {
  async fetch(request, env) {
    try {
      KV_BINDING = env.KV_BINDING;
      DB = env.DB;
      await initDatabase();
      
      const url = new URL(request.url);
      const path = url.pathname.slice(1);

      // 根目录跳转
      if (path === '') {
        return Response.redirect(url.origin + '/admin.html', 302);
      }

      // 图片上传处理
      if (path === 'api/upload-image' && request.method === 'POST') {
        if (!verifyAuthCookie(request, env)) {
          return new Response(JSON.stringify({ success: false, error: '未授权' }), { 
            status: 401,
            headers: { 'Content-Type': 'application/json' }
          });
        }

        try {
          const formData = await request.formData();
          const file = formData.get('image');
          
          if (!file) {
            return new Response(JSON.stringify({ success: false, error: '未找到图片' }), { 
              status: 400,
              headers: { 'Content-Type': 'application/json' }
            });
          }

          const arrayBuffer = await file.arrayBuffer();
          const base64 = btoa(String.fromCharCode(...new Uint8Array(arrayBuffer)));
          const mimeType = file.type || 'image/png';
          const dataUrl = `data:${mimeType};base64,${base64}`;

          return new Response(JSON.stringify({
            success: true,
            data: {
              base64,
              dataUrl,
              fileName: file.name,
              mimeType
            }
          }), { headers: { 'Content-Type': 'application/json' } });
        } catch (error) {
          return new Response(JSON.stringify({ success: false, error: error.message }), { 
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // API路由处理
      if (path.startsWith('api/')) {
        // 登录API
        if (path === 'api/login' && request.method === 'POST') {
          try {
            const body = await request.json().catch(() => ({}));
            const { password } = body;
            
            if (password === env.PASSWORD) {
              const isProd = url.protocol === 'https:';
              return new Response(JSON.stringify({ success: true }), {
                headers: {
                  'Set-Cookie': `token=${password}; Path=/; HttpOnly; SameSite=Lax; ${isProd ? 'Secure;' : ''} Max-Age=86400`,
                  'Content-Type': 'application/json'
                }
              });
            } else {
              return new Response(JSON.stringify({ success: false, error: '密码错误' }), { 
                status: 401,
                headers: { 'Content-Type': 'application/json' }
              });
            }
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: '请求格式错误' }), { 
              status: 400,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        // 登出API
        if (path === 'api/logout' && request.method === 'POST') {
          return new Response(JSON.stringify({ success: true }), {
            headers: clearAuthCookie()
          });
        }

        // 验证登录状态
        if (!verifyAuthCookie(request, env)) {
          return new Response(JSON.stringify({ success: false, error: '未授权' }), { 
            status: 401,
            headers: { 'Content-Type': 'application/json' }
          });
        }

        // 获取映射列表
        if (path === 'api/mappings' && request.method === 'GET') {
          try {
            const page = parseInt(url.searchParams.get('page') || '1');
            const pageSize = parseInt(url.searchParams.get('pageSize') || '10');
            const result = await listMappings(page, pageSize);
            return new Response(JSON.stringify({ success: true, data: result }), { 
              headers: { 'Content-Type': 'application/json' }
            });
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: error.message }), { 
              status: 500,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        // 创建映射
        if (path === 'api/mappings' && request.method === 'POST') {
          try {
            const body = await request.json();
            const { path: mappingPath, target, name, expiry, enabled = true, isWechat = false, qrCodeData, imageUrl, imageBase64, imageAlt } = body;
            await createMapping(mappingPath, target, name, expiry, enabled, isWechat, qrCodeData, imageUrl, imageBase64, imageAlt);
            return new Response(JSON.stringify({ success: true }), { 
              headers: { 'Content-Type': 'application/json' }
            });
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: error.message }), { 
              status: 400,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        // 更新映射
        if (path === 'api/mappings' && request.method === 'PUT') {
          try {
            const body = await request.json();
            const { originalPath, newPath, target, name, expiry, enabled = true, isWechat = false, qrCodeData, imageUrl, imageBase64, imageAlt } = body;
            await updateMapping(originalPath, newPath, target, name, expiry, enabled, isWechat, qrCodeData, imageUrl, imageBase64, imageAlt);
            return new Response(JSON.stringify({ success: true }), { 
              headers: { 'Content-Type': 'application/json' }
            });
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: error.message }), { 
              status: 400,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        // 删除映射
        if (path === 'api/mappings' && request.method === 'DELETE') {
          try {
            const body = await request.json();
            await deleteMapping(body.path);
            return new Response(JSON.stringify({ success: true }), { 
              headers: { 'Content-Type': 'application/json' }
            });
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: error.message }), { 
              status: 400,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        // 获取过期映射
        if (path === 'api/expiring' && request.method === 'GET') {
          try {
            const result = await getExpiringMappings();
            return new Response(JSON.stringify({ success: true, data: result }), { 
              headers: { 'Content-Type': 'application/json' }
            });
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: error.message }), { 
              status: 500,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        // 清理过期映射
        if (path === 'api/cleanup' && request.method === 'POST') {
          try {
            await cleanupExpiredMappings();
            return new Response(JSON.stringify({ success: true }), { 
              headers: { 'Content-Type': 'application/json' }
            });
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: error.message }), { 
              status: 500,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        // 数据迁移
        if (path === 'api/migrate' && request.method === 'POST') {
          try {
            await migrateFromKV();
            return new Response(JSON.stringify({ success: true }), { 
              headers: { 'Content-Type': 'application/json' }
            });
          } catch (error) {
            return new Response(JSON.stringify({ success: false, error: error.message }), { 
              status: 500,
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }

        return new Response(JSON.stringify({ success: false, error: 'API不存在' }), { 
          status: 404,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 短链接跳转处理
      try {
        const mapping = await DB.prepare('SELECT target, enabled, expiry FROM mappings WHERE path = ?').bind(path).first();
        
        if (mapping && mapping.enabled === 1) {
          if (mapping.expiry && new Date(mapping.expiry) < new Date()) {
            return new Response('该短链接已过期', { status: 410 });
          }
          return Response.redirect(mapping.target, 302);
        }
      } catch (error) {
        console.error('短链接跳转失败:', error);
      }

      // 静态资源处理
      if (env.ASSETS) {
        try {
          const asset = await env.ASSETS.fetch(request);
          if (asset.status !== 404) {
            return asset;
          }
        } catch (error) {
          console.error('静态资源获取失败:', error);
        }
      }

      return new Response('短链接不存在', { status: 404 });
    } catch (error) {
      console.error('Worker全局异常:', error);
      return new Response(`服务器内部错误: ${error.message}`, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  },
  
  async scheduled(event, env, ctx) {
    try {
      KV_BINDING = env.KV_BINDING;
      DB = env.DB;
      await initDatabase();
      await cleanupExpiredMappings();
      console.log('清理过期映射成功');
    } catch (error) {
      console.error('定时任务失败:', error);
    }
  }
};
