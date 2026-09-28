(() => {
  const state = {
    invoices: [],
    searchTimer: null,
    searchAbort: null,
    files: [],
  };

  const $ = (selector) => document.querySelector(selector);
  const loginView = $('#login-view');
  const formView = $('#form-view');
  const loginForm = $('#login-form');
  const loginError = $('#login-error');
  const reportForm = $('#delivery-report-form');
  const searchInput = $('#invoice-search');
  const results = $('#invoice-results');
  const selectedInvoices = $('#selected-invoices');
  const selectedCount = $('#selected-count');
  const imageInput = $('#images');
  const imagePreview = $('#image-preview');
  const paymentOptions = [...document.querySelectorAll('input[name="paymentMethod"]')];
  const cashField = $('#cash-amount-field');
  const cashAmount = $('#cash-amount');
  const submitButton = $('#submit-button');
  const submitMessage = $('#submit-message');

  function showForm() {
    loginView.hidden = true;
    formView.hidden = false;
  }

  function showLogin() {
    loginView.hidden = false;
    formView.hidden = true;
  }

  function setMessage(message, type = '') {
    submitMessage.textContent = message;
    submitMessage.className = `status-message ${type}`.trim();
  }

  async function request(url, options = {}) {
    const response = await fetch(url, {
      credentials: 'same-origin',
      ...options,
    });
    if (response.status === 401) {
      showLogin();
      throw new Error('Phiên đăng nhập đã hết hạn');
    }
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.message || 'Có lỗi xảy ra');
    }
    return payload;
  }

  loginForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    loginError.textContent = '';
    const password = $('#password').value;
    try {
      await request('/packing/auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password }),
      });
      loginForm.reset();
      showForm();
    } catch (error) {
      loginError.textContent = error.message;
    }
  });

  $('#logout-button').addEventListener('click', async () => {
    await fetch('/packing/logout', {
      method: 'POST',
      credentials: 'same-origin',
    });
    state.invoices = [];
    renderSelectedInvoices();
    showLogin();
  });

  function renderSelectedInvoices() {
    selectedCount.textContent = `${state.invoices.length} hóa đơn`;
    if (!state.invoices.length) {
      selectedInvoices.innerHTML = '<p class="empty-state">Chưa chọn hóa đơn.</p>';
      return;
    }

    selectedInvoices.innerHTML = state.invoices
      .map(
        (invoice) => `
          <div class="selected-invoice">
            <div>
              <strong>${escapeHtml(invoice.code)}</strong>
              <small>${escapeHtml(invoice.customerName || 'Khách lẻ')} · Người bán: ${escapeHtml(invoice.sellerName || 'Chưa xác định')}</small>
            </div>
            <button class="remove-invoice" type="button" data-remove-invoice="${invoice.id}">Xóa</button>
          </div>
        `,
      )
      .join('');
  }

  selectedInvoices.addEventListener('click', (event) => {
    const button = event.target.closest('[data-remove-invoice]');
    if (!button) return;
    const id = Number(button.dataset.removeInvoice);
    state.invoices = state.invoices.filter((invoice) => invoice.id !== id);
    renderSelectedInvoices();
  });

  function renderResults(items, message = '') {
    if (message) {
      results.innerHTML = `<div class="search-state">${escapeHtml(message)}</div>`;
      results.hidden = false;
      searchInput.setAttribute('aria-expanded', 'true');
      return;
    }

    results.innerHTML = items
      .map(
        (invoice) => `
          <button class="search-result" type="button" role="option" data-invoice-id="${invoice.id}">
            <span class="result-code">${escapeHtml(invoice.code)}</span>
            <span class="result-meta">${escapeHtml(invoice.customerName || 'Khách lẻ')} · ${escapeHtml(invoice.sellerName || 'Chưa xác định')}</span>
          </button>
        `,
      )
      .join('');
    results.hidden = items.length === 0;
    searchInput.setAttribute('aria-expanded', items.length ? 'true' : 'false');
  }

  async function searchInvoices() {
    const term = searchInput.value.trim();
    if (!term) {
      results.hidden = true;
      return;
    }

    if (state.searchAbort) state.searchAbort.abort();
    state.searchAbort = new AbortController();
    renderResults([], 'Đang tìm...');
    try {
      const response = await fetch(`/packing/invoices?search=${encodeURIComponent(term)}`, {
        credentials: 'same-origin',
        signal: state.searchAbort.signal,
      });
      if (response.status === 401) {
        showLogin();
        throw new Error('Phiên đăng nhập đã hết hạn');
      }
      if (!response.ok) throw new Error('Không tìm được hóa đơn');
      const items = await response.json();
      renderResults(items, items.length ? '' : 'Không có hóa đơn phù hợp');
    } catch (error) {
      if (error.name !== 'AbortError') renderResults([], error.message);
    }
  }

  searchInput.addEventListener('input', () => {
    clearTimeout(state.searchTimer);
    state.searchTimer = setTimeout(searchInvoices, 250);
  });

  results.addEventListener('click', async (event) => {
    const result = event.target.closest('[data-invoice-id]');
    if (!result) return;
    const id = Number(result.dataset.invoiceId);
    const term = searchInput.value.trim();
    try {
      const response = await fetch(`/packing/invoices?search=${encodeURIComponent(term)}`, {
        credentials: 'same-origin',
      });
      const items = await response.json();
      const selected = items.find((invoice) => invoice.id === id);
      if (selected && !state.invoices.some((invoice) => invoice.id === id)) {
        state.invoices.push(selected);
        renderSelectedInvoices();
      }
    } catch {
      setMessage('Không thể chọn hóa đơn', 'error');
    }
    searchInput.value = '';
    results.hidden = true;
    searchInput.setAttribute('aria-expanded', 'false');
  });

  document.addEventListener('click', (event) => {
    if (!event.target.closest('.search-field')) {
      results.hidden = true;
      searchInput.setAttribute('aria-expanded', 'false');
    }
  });

  paymentOptions.forEach((option) => {
    option.addEventListener('change', () => {
      const isCash = option.value === 'CASH' && option.checked;
      cashField.hidden = !isCash;
      cashAmount.required = isCash;
      if (!isCash) cashAmount.value = '';
    });
  });

  imageInput.addEventListener('change', () => {
    state.files = [...state.files, ...Array.from(imageInput.files || [])].slice(0, 10);
    imageInput.value = '';
    renderPreviews();
  });

  function renderPreviews() {
    imagePreview.innerHTML = '';
    state.files.forEach((file, index) => {
      const item = document.createElement('div');
      item.className = 'preview-item';
      const image = document.createElement('img');
      image.alt = file.name;
      image.src = URL.createObjectURL(file);
      const remove = document.createElement('button');
      remove.className = 'preview-remove';
      remove.type = 'button';
      remove.textContent = '×';
      remove.setAttribute('aria-label', `Xóa ${file.name}`);
      remove.addEventListener('click', () => {
        URL.revokeObjectURL(image.src);
        state.files.splice(index, 1);
        renderPreviews();
      });
      item.append(image, remove);
      imagePreview.appendChild(item);
    });
  }

  reportForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    setMessage('');
    if (!state.invoices.length) {
      setMessage('Hãy chọn ít nhất một hóa đơn', 'error');
      return;
    }

    const paymentMethod = document.querySelector('input[name="paymentMethod"]:checked').value;
    const formData = new FormData();
    formData.append('invoiceIds', JSON.stringify(state.invoices.map((invoice) => invoice.id)));
    formData.append('packageCount', $('#package-count').value);
    formData.append('paymentMethod', paymentMethod);
    formData.append('cashAmount', paymentMethod === 'CASH' ? cashAmount.value : '');
    formData.append('note', $('#note').value);
    state.files.forEach((file) => formData.append('images', file));

    submitButton.disabled = true;
    submitButton.textContent = 'Đang lưu...';
    try {
      const report = await request('/packing/delivery-reports', {
        method: 'POST',
        body: formData,
      });
      const webhookText =
        report.webhookStatus === 'SENT'
          ? 'Webhook đã gửi thành công.'
          : 'Đã lưu, webhook sẽ được tự động thử lại.';
      setMessage(`Đã tạo ${report.code}. ${webhookText}`, 'success');
      reportForm.reset();
      state.invoices = [];
      state.files = [];
      renderSelectedInvoices();
      renderPreviews();
      cashField.hidden = true;
      cashAmount.required = false;
    } catch (error) {
      setMessage(error.message, 'error');
    } finally {
      submitButton.disabled = false;
      submitButton.textContent = 'Tạo báo đơn';
    }
  });

  function escapeHtml(value) {
    return String(value)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  renderSelectedInvoices();
})();
