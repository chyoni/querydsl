package com.example.querydsl.repository;

import com.example.querydsl.dto.MemberSearchCondition;
import com.example.querydsl.dto.MemberTeamDto;

import java.util.List;

public interface MemberRepositoryQueryDsl {
    List<MemberTeamDto> search(MemberSearchCondition condition);
}
