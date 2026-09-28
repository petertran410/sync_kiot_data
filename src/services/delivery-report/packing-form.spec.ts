import { readFileSync } from 'fs';
import { join } from 'path';
import { runInNewContext } from 'vm';

type FormEvent = {
  preventDefault?: () => void;
  target?: { closest: (selector: string) => unknown };
};

class ElementStub {
  value = '';
  textContent = '';
  innerHTML = '';
  className = '';
  hidden = false;
  checked = false;
  required = false;
  disabled = false;
  selectionStart: number | null = 0;
  files: File[] = [];
  private listeners = new Map<string, (event: FormEvent) => unknown>();

  addEventListener(name: string, handler: (event: FormEvent) => unknown) {
    this.listeners.set(name, handler);
  }

  async fire(name: string, event: FormEvent = {}) {
    await this.listeners.get(name)?.(event);
  }

  setSelectionRange(start: number) {
    this.selectionStart = start;
  }

  setAttribute() {}

  reset() {}
}

describe('packing cash amount input', () => {
  function setup() {
    const elements = new Map<string, ElementStub>();
    const element = (selector: string) => {
      if (!elements.has(selector)) elements.set(selector, new ElementStub());
      return elements.get(selector)!;
    };
    const cash = element('#cash-amount');
    const cashOption = element('#cash-option');
    cashOption.value = 'CASH';
    cashOption.checked = true;
    const transferOption = element('#transfer-option');
    transferOption.value = 'TRANSFER';
    element('#package-count').value = '2';
    let submitted: FormData | undefined;
    const fetch = jest.fn((url: string, options?: { body?: FormData }) => {
      if (url === '/packing/session') return Promise.resolve({ ok: true });
      if (url.startsWith('/packing/invoices?')) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve([{ id: 101, code: 'HD000101' }]),
        });
      }
      if (url === '/packing/delivery-reports') {
        submitted = options?.body;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ code: 'BD000006' }),
        });
      }
      throw new Error(`Unexpected request: ${url}`);
    });
    const document = {
      querySelector: (selector: string) =>
        selector === 'input[name="paymentMethod"]:checked'
          ? cashOption
          : element(selector),
      querySelectorAll: () => [transferOption, cashOption],
      addEventListener: () => undefined,
    };
    runInNewContext(
      readFileSync(join(__dirname, '../../../public/packing/app.js'), 'utf8'),
      {
        document,
        fetch,
        FormData,
        Intl,
        BigInt,
        URL,
        setTimeout,
        clearTimeout,
      },
    );
    return {
      cash,
      results: element('#invoice-results'),
      form: element('#delivery-report-form'),
      submitted: () => submitted,
    };
  }

  it('groups digits as en-US while keeping the caret usable', async () => {
    const { cash } = setup();
    cash.value = '1000';
    cash.selectionStart = 4;
    await cash.fire('input');
    expect(cash.value).toBe('1,000');
    expect(cash.selectionStart).toBe(5);

    cash.value = '1234567';
    cash.selectionStart = 7;
    await cash.fire('input');
    expect(cash.value).toBe('1,234,567');
    expect(cash.selectionStart).toBe(9);

    cash.value = '1,9234';
    cash.selectionStart = 3;
    await cash.fire('input');
    expect(cash.value).toBe('19,234');
    expect(cash.selectionStart).toBe(2);

    cash.value = '1234';
    cash.selectionStart = 1;
    await cash.fire('input');
    expect(cash.value).toBe('1,234');
    expect(cash.selectionStart).toBe(1);
  });

  it('submits unformatted digits to the existing API', async () => {
    const { cash, results, form, submitted } = setup();
    await results.fire('click', {
      target: {
        closest: () => ({ dataset: { invoiceId: '101' } }),
      },
    });
    cash.value = '1,234,567';
    await form.fire('submit', { preventDefault: () => undefined });

    expect(submitted()?.get('cashAmount')).toBe('1234567');
    expect(submitted()?.get('invoiceIds')).toBe('[101]');
  });
});
