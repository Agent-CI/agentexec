import ReactPaginate from 'react-paginate';

export interface PaginationProps {
  page: number;
  totalPages: number;
  onPageChange: (page: number) => void;
  className?: string;
}

/**
 * Pagination component using react-paginate
 */
export function Pagination({ page, totalPages, onPageChange, className = '' }: PaginationProps) {
  if (totalPages <= 1) {
    return null;
  }

  const handlePageChange = (event: { selected: number }) => {
    // react-paginate uses 0-indexed pages, we use 1-indexed
    onPageChange(event.selected + 1);
  };

  return (
    <ReactPaginate
      pageCount={totalPages}
      forcePage={page - 1}
      onPageChange={handlePageChange}
      pageRangeDisplayed={3}
      marginPagesDisplayed={1}
      previousLabel="Previous"
      nextLabel="Next"
      breakLabel="..."
      containerClassName={`ax-pagination ${className}`.trim()}
      pageClassName="ax-pagination__page-item"
      pageLinkClassName="ax-pagination__page"
      activeClassName="ax-pagination__page-item--active"
      previousClassName="ax-pagination__btn-item"
      previousLinkClassName="ax-pagination__btn ax-pagination__btn--prev"
      nextClassName="ax-pagination__btn-item"
      nextLinkClassName="ax-pagination__btn ax-pagination__btn--next"
      breakClassName="ax-pagination__break-item"
      breakLinkClassName="ax-pagination__ellipsis"
      disabledClassName="ax-pagination__item--disabled"
      disabledLinkClassName="ax-pagination__link--disabled"
    />
  );
}
