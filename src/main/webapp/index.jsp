<%@ page contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%
    // Redirect ngay khi vào /
    response.sendRedirect(request.getContextPath() + "/players");
%>
