import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import ReactPaginate from 'react-paginate';
import { TaskList } from 'agentexec-ui';
import { useActivityList } from '../api/queries';

const PAGE_SIZE = 20;

export function AgentListPage() {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);

  const { data: activityList, isLoading } = useActivityList(page, PAGE_SIZE);

  const handleTaskClick = (agentId: string) => {
    navigate(`/agents/${agentId}`);
  };

  const handlePageChange = (event: { selected: number }) => {
    setPage(event.selected + 1);
  };

  return (
    <>
      <header className="main__header">
        <h2 className="main__title">Background Agents</h2>
        <p className="main__subtitle">
          {activityList ? `${activityList.total} total tasks` : 'Loading...'}
        </p>
      </header>

      <div className="main__content">
        <div className="task-panel">
          <div className="task-panel__list">
            <TaskList
              items={activityList?.items || []}
              loading={isLoading}
              onTaskClick={handleTaskClick}
            />
            {activityList && activityList.total_pages > 1 && (
              <ReactPaginate
                pageCount={activityList.total_pages}
                forcePage={page - 1}
                onPageChange={handlePageChange}
                pageRangeDisplayed={3}
                marginPagesDisplayed={1}
                previousLabel="Previous"
                nextLabel="Next"
                breakLabel="..."
                containerClassName="ax-pagination"
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
              />
            )}
          </div>
        </div>
      </div>
    </>
  );
}
