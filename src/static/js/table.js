// Immediately Invoked Function Expression (IIFE) to avoid polluting global scope
(function () {
  let isLoading = false;
  let hasMoreData = true;
  let currentPagingState = null;
  let limit = 50;

  function getKeyspaceAndTable() {
    const path = window.location.pathname;
    const match = path.match(/^\/view\/([^\/]+)\/([^\/]+)/);
    if (match) {
      return {
        keyspace: decodeURIComponent(match[1]),
        table: decodeURIComponent(match[2])
      };
    }
    return null;
  }

  const urlInfo = getKeyspaceAndTable();

  const baseUrl = urlInfo ? `/view/${encodeURIComponent(urlInfo.keyspace)}/${encodeURIComponent(urlInfo.table)}` : '';

  function loadMoreData() {
    if (isLoading || !hasMoreData || !baseUrl) return;
    isLoading = true;

    let url = `${baseUrl}?limit=${limit}`;
    if (currentPagingState) {
      url += `&paging_state=${encodeURIComponent(currentPagingState)}`;
    }

    htmx.ajax('GET', url, {
      target: '#table-body',
      swap: 'beforeend'
    }).then(() => {
      isLoading = false;
    });
  }

  // Handle limit dropdown change
  const limitSelect = document.getElementById('limit-select');
  if (limitSelect) {
    limitSelect.addEventListener('change', function () {
      limit = parseInt(this.value, 10);
      // Reset table and state, then reload data
      document.getElementById('table-body').innerHTML = '';
      hasMoreData = true;
      currentPagingState = null;
      document.getElementById('end-of-data').style.display = 'none';
      loadMoreData();
    });
  }

  // Infinite scroll detection inside .table-responsive
  const tableResponsive = document.querySelector('.table-responsive');
  if (tableResponsive) {
    tableResponsive.addEventListener('scroll', () => {
      // Check if near the bottom of the scrollable div
      if ((tableResponsive.scrollTop + tableResponsive.clientHeight) >= (tableResponsive.scrollHeight - 200)) {
        loadMoreData();
      }
    });
  }

  // Handle HTMX events
  document.body.addEventListener('htmx:afterRequest', function (evt) {
    const xhr = evt.detail.xhr;

    // Update pagination state from response headers
    const hasMore = xhr.getResponseHeader('X-Has-More-Pages');
    const pagingState = xhr.getResponseHeader('X-Paging-State');

    if (hasMore === 'false') {
      hasMoreData = false;
      document.getElementById('end-of-data').style.display = 'block';
      currentPagingState = null;
    } else if (pagingState) {
      // Store the already base64-encoded paging state
      currentPagingState = pagingState;
    }
  });

  document.body.addEventListener('htmx:beforeRequest', function (evt) {
    // Reset state for initial load
    if (!evt.detail.requestConfig.parameters || !evt.detail.requestConfig.parameters.paging_state) {
      hasMoreData = true;
      currentPagingState = null;
      document.getElementById('end-of-data').style.display = 'none';
    }
  });

  // Handle errors
  document.body.addEventListener('htmx:responseError', function (evt) {
    isLoading = false;
    console.error('Error loading data:', evt.detail);
  });

  document.addEventListener('htmx:afterSwap', function(event) {
    // Re-initialize Alpine.js for the new content
    if (typeof Alpine !== 'undefined') {
      Alpine.initTree(event.detail.elt);
    }
  });


})();
