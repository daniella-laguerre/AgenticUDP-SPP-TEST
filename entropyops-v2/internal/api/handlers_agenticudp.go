package api

import "net/http"

// GET /api/agenticudp/traversal
func (s *Server) handleAgenticUDPTraversal(w http.ResponseWriter, r *http.Request) {
	if s.agenticUDP == nil {
		writeJSON(w, map[string]interface{}{
			"enabled": false,
			"notes":   "AgenticUDP receiver is disabled for this deployment.",
		})
		return
	}
	writeJSON(w, map[string]interface{}{
		"enabled":     true,
		"diagnostics": s.agenticUDP.Diagnostics(),
	})
}
