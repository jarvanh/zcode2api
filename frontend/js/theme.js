/* zcode-hub 主题：跟随系统 / 亮色 / 暗色（localStorage 记忆）
   注意：本文件必须在 <head> 中同步加载，早于页面渲染，避免亮色用户首屏闪黑。 */
(function () {
  var KEY = 'zcode-theme';
  var ORDER = ['auto', 'light', 'dark'];

  function pref() {
    try { return localStorage.getItem(KEY) || 'auto'; } catch (e) { return 'auto'; }
  }
  function sysLight() {
    return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches);
  }
  function resolve(p) {
    return p === 'auto' ? (sysLight() ? 'light' : 'dark') : p;
  }
  function label(p) {
    return p === 'auto' ? '🖥 跟随系统' : (p === 'light' ? '☀️ 亮色' : '🌙 暗色');
  }

  // 渲染前先定好主题（防闪黑）
  try {
    document.documentElement.setAttribute('data-theme', resolve(pref()));
  } catch (e) {}

  window.zcodeTheme = {
    pref: pref,
    apply: function (p) {
      document.documentElement.setAttribute('data-theme', resolve(p));
      var btns = document.querySelectorAll('[data-theme-btn]');
      for (var i = 0; i < btns.length; i++) {
        btns[i].textContent = label(p);
        btns[i].title = '主题：' + label(p) + '（点击切换）';
      }
    },
    cycle: function () {
      var cur = pref();
      var next = ORDER[(ORDER.indexOf(cur) + 1) % ORDER.length];
      try { localStorage.setItem(KEY, next); } catch (e) {}
      window.zcodeTheme.apply(next);
    }
  };

  // 首屏按钮文案同步（header.js 渲染后再调用一次 apply 即可）
  document.addEventListener('DOMContentLoaded', function () {
    window.zcodeTheme.apply(pref());
  });

  // auto 模式下实时跟随系统深浅色变化
  if (window.matchMedia) {
    var mq = window.matchMedia('(prefers-color-scheme: light)');
    var onChange = function () { if (pref() === 'auto') window.zcodeTheme.apply('auto'); };
    if (mq.addEventListener) mq.addEventListener('change', onChange);
    else if (mq.addListener) mq.addListener(onChange);
  }
})();
