import React from "react";
import "./SignalCard.css";

export default function SignalCard({ item }) {
  const formatDate = (dateStr) => {
    if (!dateStr) return "";
    const date = new Date(dateStr);
    return date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  const renderStatusBadge = () => {
    if (item.status === 0) {
      return <span className="status-badge pending">Pending</span>;
    } else if (item.status === 1) {
      return <span className="status-badge analyzing">Analyzing</span>;
    }
    return null;
  };

  return (
    <article className="signal-card" role="article" aria-labelledby={`signal-${item.id}`}>
      <div className="signal-header">
        {item.thumbnail_url ? (
          <img
            src={item.thumbnail_url}
            alt={item.name}
            className="avatar-img"
            width={68}
            height={68}
          />
        ) : (
          <div className="avatar" aria-hidden>
            {item.name ? item.name[0].toUpperCase() : "S"}
          </div>
        )}

        <div className="signal-info">
          <div id={`signal-${item.id}`} className="signal-name">{item.name}</div>

          <div className="signal-tags">
            {/* status */}
            {(item.status === 0 || item.status === 1) && renderStatusBadge()}

            {/* signal badge */}
            {item.is_signal && <span className="signal-badge">SIGNAL</span>}

            {/* score */}
            {item.signal_strength && item.signal_score !== null && (
              <span className={`score-tag ${item.signal_strength}`}>
                {`Score: ${item.signal_score}`}
              </span>
            )}
          </div>

          {item.tagline && <div className="signal-tagline">{item.tagline}</div>}

          {item.description && (
            <div className="signal-description">{item.description}</div>
          )}

          <div className="signal-meta" aria-hidden>
            {item.created_at && <span className="meta-date">{formatDate(item.created_at)}</span>}
            <span className="meta-votes">{item.votes} votes</span>
          </div>
        </div>
      </div>

      {item.topics && item.topics.length > 0 && (
        <div className="topic-list" aria-label="topics">
          {item.topics.map((t, i) => (
            <span key={i} className="topic-chip">
              {t}
            </span>
          ))}
        </div>
      )}
    </article>
  );
}
