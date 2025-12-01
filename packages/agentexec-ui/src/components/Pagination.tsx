export interface PaginationProps {
  page: number;
  totalPages: number;
  onPageChange: (page: number) => void;
  className?: string;
}

/**
 * Pagination component for navigating between pages
 */
export function Pagination({ page, totalPages, onPageChange, className = '' }: PaginationProps) {
  if (totalPages <= 1) {
    return null;
  }

  const pages: (number | 'ellipsis')[] = [];

  // Always show first page
  pages.push(1);

  // Calculate range around current page
  const rangeStart = Math.max(2, page - 1);
  const rangeEnd = Math.min(totalPages - 1, page + 1);

  // Add ellipsis before range if needed
  if (rangeStart > 2) {
    pages.push('ellipsis');
  }

  // Add pages in range
  for (let i = rangeStart; i <= rangeEnd; i++) {
    pages.push(i);
  }

  // Add ellipsis after range if needed
  if (rangeEnd < totalPages - 1) {
    pages.push('ellipsis');
  }

  // Always show last page (if more than 1 page)
  if (totalPages > 1) {
    pages.push(totalPages);
  }

  return (
    <nav className={`ax-pagination ${className}`.trim()} aria-label="Pagination">
      <button
        className="ax-pagination__btn ax-pagination__btn--prev"
        onClick={() => onPageChange(page - 1)}
        disabled={page <= 1}
        aria-label="Previous page"
      >
        Previous
      </button>

      <div className="ax-pagination__pages">
        {pages.map((p, index) =>
          p === 'ellipsis' ? (
            <span key={`ellipsis-${index}`} className="ax-pagination__ellipsis">
              ...
            </span>
          ) : (
            <button
              key={p}
              className={`ax-pagination__page ${p === page ? 'ax-pagination__page--active' : ''}`}
              onClick={() => onPageChange(p)}
              aria-current={p === page ? 'page' : undefined}
            >
              {p}
            </button>
          )
        )}
      </div>

      <button
        className="ax-pagination__btn ax-pagination__btn--next"
        onClick={() => onPageChange(page + 1)}
        disabled={page >= totalPages}
        aria-label="Next page"
      >
        Next
      </button>
    </nav>
  );
}
